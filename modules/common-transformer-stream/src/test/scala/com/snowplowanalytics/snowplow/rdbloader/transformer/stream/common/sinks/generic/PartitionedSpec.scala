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
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sinks.generic

import scala.concurrent.duration._
import cats.implicits._
import cats.effect.{IO, Sync}
import cats.effect.kernel.Ref
import cats.effect.unsafe.implicits.global
import fs2.{Pipe, Stream}
import org.specs2.mutable.Specification

class PartitionedSpec extends Specification {
  import PartitionedSpec._

  "write" should {
    "produce consistent windows" in {

      val numWindows           = 4
      val recordsPerWindow     = 8
      val numKeys              = 2
      val itemsPerKeyPerRecord = 2

      val action = for {
        ref <- Ref.of[IO, WindowedKV](Nil)
        sinkFun   = PartitionedSpec.getSink(ref) _
        writePipe = Partitioned.write[IO, Window, Key, Value, Data](sinkFun, BufferSize)
        result <- wkvStream[IO](numWindows, numKeys, recordsPerWindow, itemsPerKeyPerRecord)
                    .through(writePipe)
                    .compile
                    .toList
                    .timeout(2000.millis)
                    .attempt
        piped <- ref.get
      } yield (result, piped)

      val (result, piped) = action.unsafeRunSync()
      result must beRight.like { case emitted =>
        emitted must beEqualTo(
          List(
            "window-0" -> List(0, 1, 2, 3, 4, 5, 6, 7),
            "window-1" -> List(0, 1, 2, 3, 4, 5, 6, 7),
            "window-2" -> List(0, 1, 2, 3, 4, 5, 6, 7),
            "window-3" -> List(0, 1, 2, 3, 4, 5, 6, 7)
          )
        )
      }

      piped must have size (numWindows * numKeys * recordsPerWindow * itemsPerKeyPerRecord)

      piped.filter(_._1 == "window-1") must have size (numKeys * recordsPerWindow * itemsPerKeyPerRecord)
    }

    "emit on completion when there is no EndWindow" in {

      val numWindows           = 1
      val recordsPerWindow     = 8
      val numKeys              = 2
      val itemsPerKeyPerRecord = 2

      // Filter out the EndWindow
      val stream = wkvStream[IO](numWindows, numKeys, recordsPerWindow, itemsPerKeyPerRecord).collect { case d @ Record.Data(_, _, _) =>
        d
      }

      val action = for {
        ref <- Ref.of[IO, WindowedKV](Nil)
        sinkFun   = PartitionedSpec.getSink(ref) _
        writePipe = Partitioned.write[IO, Window, Key, Value, Data](sinkFun, BufferSize)
        result <- stream
                    .through(writePipe)
                    .compile
                    .toList
                    .timeout(2000.millis)
                    .attempt
        piped <- ref.get

      } yield (result, piped)

      val (result, piped) = action.unsafeRunSync()

      result must beRight.like { case emitted =>
        emitted must beEqualTo(
          List(
            "window-0" -> List(0, 1, 2, 3, 4, 5, 6, 7)
          )
        )
      }

      piped must have size (numKeys * recordsPerWindow * itemsPerKeyPerRecord)
    }

  }
}

object PartitionedSpec {

  val BufferSize = 4096

  type Window     = String
  type Key        = String
  type Value      = String
  type Data       = List[Int]
  type WindowedKV = List[(Window, Key, Data, Value)]

  def getSink(ref: Ref[IO, WindowedKV])(window: Window)(data: Data)(key: Key): Pipe[IO, Value, Unit] =
    _.evalMap { value =>
      ref.update { items =>
        items :+ ((window, key, data, value))
      }
    }

  /**
   * Stream of windowed key-value paris based on stream of natural numbers Window starts at 0 and
   * has wSize kv (only last one can be smaller) Key is random number between 1 and kSize
   * (inclusive) Value starts at 0 and ever growing
   */
  def wkvStream[F[_]: Sync](
    windows: Int              = 2,
    keys: Int                 = 2,
    recordsPerWindow: Int     = 2,
    itemsPerKeyPerRecord: Int = 1
  ): Stream[F, Record[Window, List[(Key, Value)], Data]] =
    Stream
      .range(0, windows)
      .flatMap { w =>
        val window = s"window-$w"
        val datas = Stream
          .range(0, recordsPerWindow)
          .map { r =>
            val items = for {
              k <- (0 until keys).toList
              v <- (0 until itemsPerKeyPerRecord).toList
            } yield ((s"key-$k", s"value-$r-$v"))
            Record.Data[Window, List[(Key, Value)], Data](window, items, List(r))
          }
        datas ++ Stream.emit(Record.EndWindow)
      }

}
