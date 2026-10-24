/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.batch

import java.time.Instant
import java.util.{Comparator, PriorityQueue, UUID}

import scala.annotation.tailrec
import scala.util.control.NonFatal

import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputExceededException

import io.circe.literal._

import org.apache.spark.rdd.RDD

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}

import com.snowplowanalytics.snowplow.rdbloader.transformer.batch.Config.{Deduplication => DedupConfig}

object Deduplication {

  /** Arbitrary UUID, used as a dummy event id for all bad rows */
  val BadRowId = UUID.randomUUID()

  val DuplicateSchema = SchemaKey("com.snowplowanalytics.snowplow", "duplicate", "jsonschema", SchemaVer.Full(1, 0, 0))

  private val EtlTstampOrdering: Ordering[Option[Instant]] = Ordering[Option[Instant]]

  /**
   * Of two natural duplicates, pick the one natural deduplication keeps: the earlier etl_tstamp, or
   * `a` if the two are equal. A missing etl_tstamp sorts before any timestamp.
   *
   * Pairwise rather than over a whole group, so that Spark can combine as it goes. Spark's merge
   * order is unspecified, so which of several equally early events survives is not deterministic.
   */
  private[batch] def earliestByEtlTstamp(a: Event, b: Event): Event =
    if (EtlTstampOrdering.lteq(a.etl_tstamp, b.etl_tstamp)) a else b

  /**
   * What natural deduplication groups by: two events sharing a key are natural duplicates.
   *
   * `Position` is a separate case so that it cannot collide with a real event_fingerprint, which is
   * free text.
   */
  private[batch] sealed trait NaturalKey extends Product with Serializable
  private[batch] object NaturalKey {
    final case class Fingerprint(eventId: UUID, fingerprint: String) extends NaturalKey
    final case class Position(partition: Int, record: Int) extends NaturalKey
  }

  /**
   * Key one input partition's events for natural deduplication.
   *
   * An event with no event_fingerprint is keyed by where it sits in the input: unique to that
   * event, so it is never deduplicated, and the same again if Spark recomputes the partition. That
   * matters because after a fetch failure Spark recomputes only the map partitions it lost and
   * keeps the reduce partitions that already succeeded, so an event that hashed somewhere else the
   * second time could reach the output twice or not at all. Spark cannot catch it for us: the
   * overload of mapPartitionsWithIndex that declares a stage order-sensitive is private to Spark,
   * so the scheduler treats this stage as deterministic whatever we do here.
   *
   * Requires events in the order the file scan produced them. A shuffle, repartition or sort
   * between the scan and this call makes the offsets meaningless.
   */
  private[batch] def naturalKeys(partition: Int, events: Iterator[Event]): Iterator[(NaturalKey, Event)] =
    events.zipWithIndex.map { case (event, record) =>
      val key = event.event_fingerprint match {
        case Some(fingerprint) => NaturalKey.Fingerprint(event.event_id, fingerprint)
        case None              => NaturalKey.Position(partition, record)
      }
      (key, event)
    }

  /** Add duplicate context */
  def withSynthetic(event: Event): Event = {
    val newContext      = SelfDescribingData(DuplicateSchema, json"""{"originalEventId":${event.event_id}}""")
    val updatedContexts = newContext :: event.derived_contexts.data
    val newEventId      = UUID.randomUUID()
    event.copy(event_id = newEventId, derived_contexts = Contexts(updatedContexts))
  }

  /**
   * The event ids that synthetic deduplication rewrites, packed into primitives.
   *
   * This is broadcast, so whatever it costs is paid once on the driver and again on every
   * executor's heap, and it is consulted once per event. With the event_fingerprint enrichment
   * enabled it holds only genuine event id collisions, a handful at most, but disabling that
   * enrichment is a supported configuration and in it natural deduplication can match nothing, so
   * every re-delivered event lands here and the size grows with the batch. An id costs 16 bytes as
   * a pair of longs, against around 73 as a member of a Set[UUID].
   *
   * All 128 bits of an id are kept. Truncating to a single long would make the array natively
   * sortable, but at 25 million ids two of them would collide with probability around 1.7e-5, and a
   * collision here rewrites the event_id of an event that is not a duplicate at all.
   */
  final class SyntheticDupeIds private (private val packed: Array[Long]) extends Serializable {

    /**
     * Packed entries, so an id count only while the source has no repeats, as reduceByKey leaves it
     */
    def size: Int = packed.length / 2

    def contains(id: UUID): Boolean = {
      val msb = id.getMostSignificantBits
      val lsb = id.getLeastSignificantBits

      @tailrec def search(low: Int, high: Int): Boolean =
        if (low > high) false
        else {
          val middle     = (low + high) >>> 1
          val comparison = SyntheticDupeIds.compareIds(packed(middle * 2), packed(middle * 2 + 1), msb, lsb)
          if (comparison < 0) search(middle + 1, high)
          else if (comparison > 0) search(low, middle - 1)
          else true
        }

      search(0, size - 1)
    }
  }

  object SyntheticDupeIds {

    val empty: SyntheticDupeIds = new SyntheticDupeIds(new Array[Long](0))

    /**
     * Bring the ids to the driver.
     *
     * Each partition packs and sorts its own ids, so what crosses the wire and what the driver
     * holds are primitives throughout. Collecting boxed UUIDs and packing them afterwards would
     * still peak at the cost this type exists to avoid, on the smallest heap in the job.
     */
    def collectFrom(ids: RDD[UUID]): SyntheticDupeIds =
      fromRuns(ids.mapPartitions(partition => Iterator(packSorted(partition))).collect())

    private[batch] def fromRuns(runs: Array[Array[Long]]): SyntheticDupeIds =
      new SyntheticDupeIds(merge(runs))

    /** Pack one partition's ids into ascending (most significant, least significant) pairs */
    private[batch] def packSorted(ids: Iterator[UUID]): Array[Long] = {
      val sorted = ids.toArray
      java.util.Arrays.sort(sorted, UuidOrder)
      val packed = new Array[Long](sorted.length * 2)
      var i = 0
      while (i < sorted.length) {
        packed(i * 2)     = sorted(i).getMostSignificantBits
        packed(i * 2 + 1) = sorted(i).getLeastSignificantBits
        i += 1
      }
      packed
    }

    /**
     * Merge runs that are each already in ascending order into one ascending array.
     *
     * A heap over the runs' heads, so the driver allocates one boxed index per run and nothing per
     * id, and compares each id against the number of runs rather than against their total length.
     * The indices are boxed up front because PriorityQueue takes references, and Integer.valueOf
     * only caches up to 127, so boxing them as they are polled would cost an object an id.
     * Concatenating the runs and sorting the whole array instead is not open to us, because sorting
     * an Array[Long] orders the interleaved halves of an id as if they were ids of their own.
     */
    private[batch] def merge(runs: Array[Array[Long]]): Array[Long] = {
      val cursors = new Array[Int](runs.length)
      val byHead = new Comparator[Integer] {
        def compare(a: Integer, b: Integer): Int = {
          val i = a.intValue
          val j = b.intValue
          compareIds(runs(i)(cursors(i)), runs(i)(cursors(i) + 1), runs(j)(cursors(j)), runs(j)(cursors(j) + 1))
        }
      }
      // A run's head only moves while that run is out of the heap, so the ordering the heap was
      // built on never changes underneath it
      val heads   = new PriorityQueue[Integer](math.max(runs.length, 1), byHead)
      val indices = Array.tabulate(runs.length)(i => Integer.valueOf(i))
      var run = 0
      while (run < runs.length) {
        if (runs(run).length > 0) heads.add(indices(run))
        run += 1
      }

      val merged = new Array[Long](runs.foldLeft(0)(_ + _.length))
      var next = 0
      while (!heads.isEmpty) {
        val from = heads.poll().intValue
        merged(next)     = runs(from)(cursors(from))
        merged(next + 1) = runs(from)(cursors(from) + 1)
        next += 2
        cursors(from) += 2
        if (cursors(from) < runs(from).length) heads.add(indices(from))
      }
      merged
    }

    /** Ascending order over ids, shared by the per-partition sort, the merge and the search */
    private def compareIds(
      msbA: Long,
      lsbA: Long,
      msbB: Long,
      lsbB: Long
    ): Int = {
      val byMsb = java.lang.Long.compare(msbA, msbB)
      if (byMsb != 0) byMsb else java.lang.Long.compare(lsbA, lsbB)
    }

    private val UuidOrder: Comparator[UUID] = new Comparator[UUID] {
      def compare(a: UUID, b: UUID): Int =
        compareIds(a.getMostSignificantBits, a.getLeastSignificantBits, b.getMostSignificantBits, b.getLeastSignificantBits)
    }
  }

  /**
   * What deduplication produced: the events to carry forward, and the event ids among them that
   * synthetic deduplication should rewrite.
   *
   * Broadcast mode leaves the events alone and names the ids, Join mode rewrites the events itself
   * and names none, and disabled mode does neither.
   *
   * @param events
   *   parsed dataset of events, can be original or deduplicated
   * @param duplicates
   *   event ids to rewrite as synthetic duplicates, possibly none
   */
  final case class Result(events: RDD[Either[BadRow, Event]], duplicates: SyntheticDupeIds)

  /** Run a deduplication process against a dataset */
  def sytheticDeduplication(config: DedupConfig, events: RDD[Either[BadRow, Event]]): Result = {
    val cardinality = config.synthetic match {
      case DedupConfig.Synthetic.Broadcast(c) => c
      case _                                  => 1
    }
    // Count synthetic duplicates, defined as events with the same id but different fingerprints
    // Won't be executed in case of None
    val duplicates = events
      .flatMap {
        case Right(e) => Some((e.event_id, 1L))
        case Left(_)  => None
      }
      .reduceByKey(_ + _)
      .flatMap {
        case (id, count) if count > cardinality => Some(id)
        case _                                  => None
      }

    config.synthetic match {
      case DedupConfig.Synthetic.None =>
        Result(events, SyntheticDupeIds.empty)
      case DedupConfig.Synthetic.Join =>
        val rdd = events
          .map {
            case Right(event) => event.event_id -> Right(event)
            case Left(badRow) => BadRowId -> Left(badRow)
          }
          .leftOuterJoin(duplicates.map((id: UUID) => (id, ())))
          .map {
            case (_, (Right(event), d)) if d.isDefined => Right(withSynthetic(event))
            case (_, (Right(event), _))                => Right(event)
            case (_, (Left(badRow), _))                => Left(badRow)
          }
        Result(rdd, SyntheticDupeIds.empty)
      case DedupConfig.Synthetic.Broadcast(_) =>
        Result(events, SyntheticDupeIds.collectFrom(duplicates))
    }
  }

  /**
   * Try to store event components in duplicate storage and check if it was stored before If event
   * is unique in storage - true will be returned, If event is already in storage, with different
   * etlTstamp - false will be returned, If event is already in storage, but with same etlTstamp -
   * true will be returned (previous shredding was interrupted), If storage is not configured - true
   * will be returned. If provisioned throughput exception happened - interrupt whole job If other
   * runtime exception happened - failure is returned to be used as bad row
   * @param event
   *   whole enriched event with possibly faked fingerprint
   * @param tstamp
   *   the ETL tstamp, an earliest timestamp in a batch
   * @param duplicateStorage
   *   object dealing with possible duplicates
   * @return
   *   boolean inside validation, denoting presence or absence of event in storage
   */
  @throws[RuntimeException]
  def crossBatch(
    event: Event,
    tstamp: Instant,
    duplicateStorage: Option[EventsManifest]
  ): Either[BadRow, Boolean] =
    (event, duplicateStorage) match {
      case (_, Some(storage)) =>
        try
          Right(storage.put(event.event_id, event.event_fingerprint.getOrElse(UUID.randomUUID().toString), tstamp))
        catch {
          case e: ProvisionedThroughputExceededException =>
            throw e
          case NonFatal(e) =>
            val payload = Payload.LoaderPayload(event)
            Left(BadRow.LoaderRuntimeError(ShredJob.BadRowsProcessor, Option(e.getMessage).getOrElse(e.toString), payload))
        }
      case _ => Right(true)
    }
}
