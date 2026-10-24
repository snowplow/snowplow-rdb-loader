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
import java.util.UUID

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import org.scalacheck.{Gen, Prop}
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class DeduplicationSpec extends Specification with ScalaCheck {

  private val collectorTstamp = Instant.parse("2026-09-12T12:00:00Z")

  private def event(etlTstamp: Option[Instant]): Event =
    Event.minimal(UUID.randomUUID(), collectorTstamp, "v_collector", "v_etl").copy(etl_tstamp = etlTstamp)

  private def at(tstamp: String): Event = event(Some(Instant.parse(tstamp)))

  private def minimal(eventId: UUID): Event = Event.minimal(eventId, collectorTstamp, "v_collector", "v_etl")

  private def fingerprinted(eventId: UUID, fingerprint: String): Event =
    minimal(eventId).copy(event_fingerprint = Some(fingerprint))

  /** Ids drawn from a pool of halves small enough that many of them share one half with another */
  private val genClusteredId: Gen[UUID] = {
    val half = Gen.oneOf(0L, 1L, 2L, -1L, Long.MinValue, Long.MaxValue)
    for {
      msb <- half
      lsb <- half
    } yield new UUID(msb, lsb)
  }

  /**
   * Build the ids the way the job does: each partition packs and sorts its own, and the driver
   * merges the runs
   */
  private def collected(ids: Seq[UUID], partitions: Int): Deduplication.SyntheticDupeIds =
    Deduplication.SyntheticDupeIds.fromRuns(
      Array.tabulate(partitions) { partition =>
        val ofPartition = ids.zipWithIndex.collect { case (id, index) if index % partitions == partition => id }
        Deduplication.SyntheticDupeIds.packSorted(ofPartition.iterator)
      }
    )

  private def agrees(
    present: Seq[UUID],
    absent: Seq[UUID],
    partitions: Int
  ) = {
    val packed = collected(present.distinct, partitions)

    (present.filterNot(packed.contains) must beEmpty)
      .and(absent.filter(packed.contains) must beEmpty)
      .and(packed.size mustEqual present.distinct.size)
  }

  private val genGroup: Gen[List[Event]] =
    Gen.nonEmptyListOf(Gen.option(Gen.choose(0L, 1000000L).map(Instant.ofEpochMilli))).map(_.map(event))

  "Deduplication.earliestByEtlTstamp" should {
    "keep the event with the earliest etl_tstamp, whichever way round the arguments are" in {
      val earlier = at("2026-09-12T11:00:00Z")
      val later   = at("2026-09-12T11:30:00Z")

      Deduplication.earliestByEtlTstamp(earlier, later) must beEqualTo(earlier)
      Deduplication.earliestByEtlTstamp(later, earlier) must beEqualTo(earlier)
    }

    "treat a missing etl_tstamp as earlier than any timestamp" in {
      val missing = event(None)
      val present = at("2026-09-12T11:00:00Z")

      Deduplication.earliestByEtlTstamp(missing, present) must beEqualTo(missing)
      Deduplication.earliestByEtlTstamp(present, missing) must beEqualTo(missing)
    }

    "keep the first of two events that are both missing an etl_tstamp" in {
      val first  = event(None)
      val second = event(None)

      Deduplication.earliestByEtlTstamp(first, second) must beEqualTo(first)
    }

    "keep the first of two events with equal etl_tstamps" in {
      val first  = at("2026-09-12T11:00:00Z")
      val second = at("2026-09-12T11:00:00Z")

      Deduplication.earliestByEtlTstamp(first, second) must beEqualTo(first)
    }

    "reduce a group whose earliest etl_tstamp is shared by two events to the first of them" in {
      val later        = at("2026-09-12T12:00:00Z")
      val earliest     = at("2026-09-12T11:00:00Z")
      val alsoEarliest = at("2026-09-12T11:00:00Z")
      val events       = List(later, earliest, alsoEarliest)

      events.reduce(Deduplication.earliestByEtlTstamp) must beEqualTo(earliest)
    }

    "reduce any group, in any order, to an event holding the group's earliest etl_tstamp" in {
      Prop.forAll(genGroup) { group =>
        group.reduce(Deduplication.earliestByEtlTstamp).etl_tstamp must beEqualTo(group.map(_.etl_tstamp).min)
      }
    }

    "reach the same earliest etl_tstamp when the group is reduced in two halves and the halves combined" in {
      Prop.forAll(genGroup, genGroup) { (left, right) =>
        val halves = Deduplication.earliestByEtlTstamp(
          left.reduce(Deduplication.earliestByEtlTstamp),
          right.reduce(Deduplication.earliestByEtlTstamp)
        )

        halves.etl_tstamp must beEqualTo((left ++ right).map(_.etl_tstamp).min)
      }
    }
  }

  "Deduplication.naturalKeys" should {
    "key an event that has an event_fingerprint by its event_id and that fingerprint" in {
      val event = fingerprinted(UUID.randomUUID(), "bed9a39a0917874d2ff072033a6413d9")

      Deduplication.naturalKeys(3, Iterator(event)).toList must beEqualTo(
        List(Deduplication.NaturalKey.Fingerprint(event.event_id, "bed9a39a0917874d2ff072033a6413d9") -> event)
      )
    }

    "give two events sharing an event_id and an event_fingerprint the same key" in {
      val eventId = UUID.randomUUID()
      val events  = List(fingerprinted(eventId, "abc"), fingerprinted(eventId, "abc"))

      Deduplication.naturalKeys(0, events.iterator).map(_._1).toList.distinct must haveSize(1)
    }

    "give two events sharing an event_id but with different event_fingerprints different keys" in {
      val eventId = UUID.randomUUID()
      val events  = List(fingerprinted(eventId, "abc"), fingerprinted(eventId, "def"))

      Deduplication.naturalKeys(0, events.iterator).map(_._1).toList.distinct must haveSize(2)
    }

    "give every event with no event_fingerprint a key of its own, even when they share an event_id" in {
      val eventId = UUID.randomUUID()
      val events  = List.fill(3)(minimal(eventId))

      Deduplication.naturalKeys(0, events.iterator).map(_._1).toList.distinct must haveSize(3)
    }

    "keep a key derived from a position distinct from one derived from a fingerprint that spells out the same position" in {
      val eventId = UUID.randomUUID()
      val events  = List(minimal(eventId), minimal(eventId), fingerprinted(eventId, "0"), fingerprinted(eventId, "1"))

      Deduplication.naturalKeys(0, events.iterator).map(_._1).toList.distinct must haveSize(4)
    }

    "give the same keys on a second pass over the same events" in {
      val events = List(fingerprinted(UUID.randomUUID(), "abc"), minimal(UUID.randomUUID()), minimal(UUID.randomUUID()))

      val firstPass  = Deduplication.naturalKeys(7, events.iterator).map(_._1).toList
      val secondPass = Deduplication.naturalKeys(7, events.iterator).map(_._1).toList

      firstPass must beEqualTo(secondPass)
    }

    "give the same event in two input partitions different keys when it has no event_fingerprint" in {
      val event = minimal(UUID.randomUUID())

      val inPartitionZero = Deduplication.naturalKeys(0, Iterator(event)).map(_._1).toList
      val inPartitionOne  = Deduplication.naturalKeys(1, Iterator(event)).map(_._1).toList

      inPartitionZero must not(beEqualTo(inPartitionOne))
    }

    "stream rather than buffer the partition it is given" in {
      val endless = Iterator.continually(minimal(UUID.randomUUID()))

      Deduplication.naturalKeys(0, endless).take(2).size must beEqualTo(2)
    }

    "pair every key with the event it came from" in {
      val events = List(minimal(UUID.randomUUID()), fingerprinted(UUID.randomUUID(), "abc"))

      Deduplication.naturalKeys(0, events.iterator).map(_._2).toList must beEqualTo(events)
    }
  }

  "Deduplication.SyntheticDupeIds" should {
    "find nothing at all in an empty set of ids" in {
      val ofNoPartitions = Deduplication.SyntheticDupeIds.empty
      val ofEmptyRuns    = collected(Nil, 4)

      (ofNoPartitions.contains(UUID.randomUUID()) must beFalse)
        .and(ofEmptyRuns.contains(UUID.randomUUID()) must beFalse)
        .and(ofEmptyRuns.size mustEqual 0)
    }

    "find the only id of a single element set, and nothing either side of it" in {
      agrees(present = List(new UUID(4L, 5L)), absent = List(new UUID(4L, 4L), new UUID(4L, 6L), new UUID(3L, 5L)), partitions = 1)
    }

    "distinguish ids that differ only in their least significant long" in {
      agrees(
        present    = List(new UUID(9L, 3L), new UUID(9L, 1L)),
        absent     = List(new UUID(9L, 0L), new UUID(9L, 2L), new UUID(9L, 4L)),
        partitions = 2
      )
    }

    "distinguish ids that differ only in their most significant long" in {
      agrees(
        present    = List(new UUID(1L, 9L), new UUID(3L, 9L)),
        absent     = List(new UUID(0L, 9L), new UUID(2L, 9L), new UUID(4L, 9L)),
        partitions = 2
      )
    }

    "order the halves of an id as signed longs, so that ids with negative halves are still found" in {
      agrees(
        present    = List(new UUID(Long.MinValue, 0L), new UUID(-1L, -1L), new UUID(0L, 0L), new UUID(Long.MaxValue, -2L)),
        absent     = List(new UUID(Long.MinValue, 1L), new UUID(-1L, 0L), new UUID(0L, -1L), new UUID(Long.MaxValue, -1L)),
        partitions = 3
      )
    }

    "merge runs into one ascending run" in {
      val runs = Array(
        List(new UUID(5L, 1L), new UUID(9L, 2L)),
        List(new UUID(1L, 7L), new UUID(5L, 0L), new UUID(9L, 1L)),
        List.empty[UUID]
      ).map(ids => Deduplication.SyntheticDupeIds.packSorted(ids.iterator))

      val merged = Deduplication.SyntheticDupeIds.merge(runs)
      val ids    = (0 until merged.length / 2).map(i => new UUID(merged(i * 2), merged(i * 2 + 1))).toList

      ids must beEqualTo(List(new UUID(1L, 7L), new UUID(5L, 0L), new UUID(5L, 1L), new UUID(9L, 1L), new UUID(9L, 2L)))
    }

    "merge runs of very unequal length, including an empty one" in {
      val many   = (1 to 500).map(i => new UUID(0L, i.toLong)).toList
      val one    = List(new UUID(0L, 1000L))
      val runs   = Array(many, one, List.empty[UUID]).map(ids => Deduplication.SyntheticDupeIds.packSorted(ids.iterator))
      val packed = Deduplication.SyntheticDupeIds.fromRuns(runs)

      ((many ++ one).filterNot(packed.contains) must beEmpty)
        .and(packed.size mustEqual (many ++ one).size)
        .and(packed.contains(new UUID(0L, 999L)) must beFalse)
    }

    "agree with a Set[UUID] over a large sample of random ids spread across many partitions" in {
      val random  = new scala.util.Random(20260916)
      val present = List.fill(100000)(new UUID(random.nextLong(), random.nextLong())).distinct
      val absent  = List.fill(100000)(new UUID(random.nextLong(), random.nextLong()))
      val oracle  = present.toSet
      val packed  = collected(present, 64)

      val disagreements = (present ++ absent).filter(id => packed.contains(id) != oracle.contains(id))

      (disagreements must beEmpty).and(packed.size mustEqual oracle.size)
    }

    "agree with a Set[UUID] over ids drawn from a pool small enough that they share halves" in {
      Prop.forAllNoShrink(Gen.listOf(genClusteredId), Gen.listOf(genClusteredId), Gen.choose(1, 8)) { (present, probes, partitions) =>
        val oracle = present.toSet
        val packed = collected(present.distinct, partitions)

        val disagreements = (present ++ probes).filter(id => packed.contains(id) != oracle.contains(id))

        (disagreements must beEmpty).and(packed.size mustEqual oracle.size)
      }
    }
  }
}
