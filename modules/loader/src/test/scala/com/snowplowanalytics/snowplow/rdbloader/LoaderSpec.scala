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
package com.snowplowanalytics.snowplow.rdbloader

import java.time.Instant

import scala.concurrent.duration._

import cats.effect.{IO, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.effect.unsafe.implicits.global

import fs2.Stream

import com.snowplowanalytics.snowplow.rdbloader.common.LoaderMessage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import com.snowplowanalytics.snowplow.rdbloader.common.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.Compression
import com.snowplowanalytics.snowplow.rdbloader.discovery.DataDiscovery
import com.snowplowanalytics.snowplow.rdbloader.dsl.metrics.Metrics
import com.snowplowanalytics.snowplow.rdbloader.dsl.{Alert, Logging, Monitoring}
import com.snowplowanalytics.snowplow.rdbloader.state.{Control, State}

import org.specs2.mutable.Specification

/**
 * Covers what this module actually promises about acking: a message is acked once the folder has
 * settled, and is left on the queue whenever it has not.
 */
class LoaderSpec extends Specification {

  import LoaderSpec._

  implicit val logging: Logging[IO] = Logging.noOp[IO]

  "handleMessage" should {
    "ack a message with nothing to load" in {
      val result = run(_ => IO.pure(DataDiscovery.ClassifiedMessage.NothingToLoad))(_ => IO.unit)

      result.acks must beEqualTo(1)
      result.alerts must beEmpty
    }

    "alert and ack a folder it has given up discovering" in {
      val error  = LoaderError.DiscoveryError(cats.data.NonEmptyList.of(discoveryFailure))
      val result = run(_ => IO.pure(DataDiscovery.ClassifiedMessage.Failed(folder, error)))(_ => IO.unit)

      result.acks must beEqualTo(1)
      result.alerts must beLike { case List(Alert.TerminalLoadFailure(_, _)) => ok }
    }

    "ack a folder that loaded" in {
      val result = run(_ => IO.pure(discovered))(_ => IO.unit)

      result.acks must beEqualTo(1)
    }

    // The two cases this whole restructuring exists for
    "leave the message on the queue when the load raises" in {
      val result = run(_ => IO.pure(discovered))(_ => IO.raiseError(new RuntimeException("boom")))

      result.acks must beEqualTo(0)
    }

    "leave the message on the queue when the load is cancelled" in {
      val gathered = for {
        acks <- Ref.of[IO, Int](0)
        alerts <- Ref.of[IO, List[Alert]](Nil)
        state <- State.mk[IO]
        control <- Control[IO](state)
        implicit0(monitoring: Monitoring[IO]) = recordingMonitoring(alerts)
        fiber <- Loader
                   .handleMessage[IO](control, _ => IO.pure(discovered), _ => IO.never)(message(acks))
                   .start
        _ <- IO.sleep(1.second)
        _ <- fiber.cancel
        acked <- acks.get
      } yield acked

      TestControl.executeEmbed(gathered).unsafeRunSync() must beEqualTo(0)
    }

    "record the queue message's sent timestamp against the loader state" in {
      // Replaces the coverage lost with DataDiscoverySpec's deleted "copy sentTimestamp" test
      val gathered = for {
        acks <- Ref.of[IO, Int](0)
        alerts <- Ref.of[IO, List[Alert]](Nil)
        state <- State.mk[IO]
        control <- Control[IO](state)
        implicit0(monitoring: Monitoring[IO]) = recordingMonitoring(alerts)
        _ <- Loader.handleMessage[IO](control, _ => IO.pure(discovered), _ => IO.unit)(message(acks))
        s <- state.get
      } yield (s.messages, s.latestMessageSentAt)

      gathered.unsafeRunSync() must beEqualTo((1, Some(sentAt)))
    }

    "hold the visibility extension open for the whole time it has the message" in {
      val gathered = for {
        acks <- Ref.of[IO, Int](0)
        alerts <- Ref.of[IO, List[Alert]](Nil)
        extending <- Ref.of[IO, Boolean](false)
        heldDuringLoad <- Ref.of[IO, Boolean](false)
        state <- State.mk[IO]
        control <- Control[IO](state)
        implicit0(monitoring: Monitoring[IO]) = recordingMonitoring(alerts)
        msg                                   = message(acks, Resource.make(extending.set(true))(_ => extending.set(false)))
        load                                  = (_: DataDiscovery.WithOrigin) => extending.get.flatMap(heldDuringLoad.set)
        _ <- Loader.handleMessage[IO](control, _ => IO.pure(discovered), load)(msg)
        during <- heldDuringLoad.get
        after <- extending.get
      } yield (during, after)

      gathered.unsafeRunSync() must beEqualTo((true, false))
    }
  }
}

object LoaderSpec {

  val folder: BlobStorage.Folder = BlobStorage.Folder.coerce("s3://bucket/folder/")
  val sentAt: Instant            = Instant.ofEpochMilli(1234567890000L)

  val discoveryFailure: com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure =
    com.snowplowanalytics.snowplow.rdbloader.discovery.DiscoveryFailure.JsonpathDiscoveryFailure("com.acme/event_a_1.json")

  val shreddingComplete: LoaderMessage.ShreddingComplete = LoaderMessage.ShreddingComplete(
    folder,
    LoaderMessage.TypesInfo.WideRow(LoaderMessage.TypesInfo.WideRow.WideRowFormat.JSON, List.empty),
    LoaderMessage.Timestamps(Instant.EPOCH, Instant.EPOCH, Some(Instant.EPOCH), Some(Instant.EPOCH)),
    Compression.Gzip,
    LoaderMessage.Processor("test-shredder", Semver(1, 0, 0)),
    None
  )

  val discovered: DataDiscovery.ClassifiedMessage.Discovered =
    DataDiscovery.ClassifiedMessage.Discovered(
      DataDiscovery.WithOrigin(
        DataDiscovery(folder, Nil, Compression.Gzip, shreddingComplete.typesInfo, Nil, Map.empty),
        shreddingComplete
      )
    )

  final case class Gathered(acks: Int, alerts: List[Alert])

  def message(
    acks: Ref[IO, Int],
    extend: Resource[IO, Unit] = Resource.unit[IO]
  ): Queue.Consumer.Message[IO] =
    new Queue.Consumer.Message[IO] {
      def content: String                      = "irrelevant -- classification is stubbed"
      def ack: IO[Unit]                        = acks.update(_ + 1)
      def sentTimestamp: Option[Instant]       = Some(sentAt)
      def extendVisibility: Resource[IO, Unit] = extend
    }

  def recordingMonitoring(alerts: Ref[IO, List[Alert]]): Monitoring[IO] =
    new Monitoring[IO] {
      def trackException(e: Throwable): IO[Unit]                = IO.unit
      def reportMetrics(metrics: Metrics.KVMetrics): IO[Unit]   = IO.unit
      def success(payload: Monitoring.SuccessPayload): IO[Unit] = IO.unit
      def alert(message: Alert): IO[Unit]                       = alerts.update(_ :+ message)
      def periodicMetrics: Metrics.PeriodicMetrics[IO] =
        new Metrics.PeriodicMetrics[IO] {
          def report: Stream[IO, Unit]                                = Stream.empty
          def setMaxTstampOfLoadedData(tstamp: Instant): IO[Unit]     = IO.unit
          def setEarliestKnownUnloadedData(tstamp: Instant): IO[Unit] = IO.unit
        }
    }

  /** Run handleMessage with classification and loading stubbed, and report what it did. */
  def run(
    classify: String => IO[DataDiscovery.ClassifiedMessage]
  )(
    load: DataDiscovery.WithOrigin => IO[Unit]
  ): Gathered = {
    implicit val logging: Logging[IO] = Logging.noOp[IO]
    val gathered = for {
      acks <- Ref.of[IO, Int](0)
      alerts <- Ref.of[IO, List[Alert]](Nil)
      state <- State.mk[IO]
      control <- Control[IO](state)
      implicit0(monitoring: Monitoring[IO]) = recordingMonitoring(alerts)
      _ <- Loader.handleMessage[IO](control, classify, load)(message(acks)).attempt
      a <- acks.get
      al <- alerts.get
    } yield Gathered(a, al)
    gathered.unsafeRunSync()
  }
}
