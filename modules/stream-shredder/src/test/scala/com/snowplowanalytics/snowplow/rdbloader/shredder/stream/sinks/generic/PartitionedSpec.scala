/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.shredder.stream.sinks.generic

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{Timer, IO, Sync, ContextShift}

import fs2.{Stream, Pipe}

import org.specs2.mutable.Specification

class PartitionedSpec extends Specification {
  import PartitionedSpec._

  "write" should {
    "produce consistent windows" in {
      implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

      def onComplete(w: Window): IO[Unit] = {
        val _ = w
        IO.unit
      }

      val action = for {
        ref       <- Ref.of[IO, WindowedKV](Map.empty)
        sinkFun    = PartitionedSpec.getSink(ref) _
        writePipe  = Partitioned.write[IO, Window, Key, Value](sinkFun, onComplete)
        _         <- wkvStream[IO].through(writePipe).flatMap(_.sink).compile.drain.timeout(200.millis).attempt
        result    <- ref.get
      } yield result


      val result = action.unsafeRunSync()
      result
        .toList
        .map { case (_, keyed) =>
          val isFull = isFullWindow(keyed)
          if (!isFull) println(result)
          isFull
        }
        .forall(identity) must beTrue
    }

    "execute onComplete callback after every window" in {
      implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

      val action = for {
        sinkRef <- Ref.of[IO, WindowedKV](Map.empty)
        windowsRef <- Ref.of[IO, List[Window]](Nil)
        sinkFun = PartitionedSpec.getSink(sinkRef) _
        writePipe = Partitioned.write[IO, Window, Key, Value](sinkFun, saveWindow(windowsRef))
        _ <- wkvStream[IO].through(writePipe).flatMap(_.sink).compile.drain.timeout(1000.millis).attempt
        sink <- sinkRef.get // sinkRef can contain +1 if window hasn't been processed yet
        windows <- windowsRef.get
      } yield (windows, sink)

      val (processedWindows, sink) = action.unsafeRunSync()
      val startedWindows = sink.toList.map { case (window, _) => window }
      val unprocessedWindows = startedWindows.diff(processedWindows)

      ((unprocessedWindows must haveSize(0)) or (unprocessedWindows must haveSize(1))) and (processedWindows.diff(startedWindows) must haveSize(0))
    }

    "not execute onComplete without EndWindow" in {
      implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
      implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

      val action = for {
        sinkRef <- Ref.of[IO, WindowedKV](Map.empty)
        windowsRef <- Ref.of[IO, List[Window]](Nil)
        sinkFun = PartitionedSpec.getSink(sinkRef) _
        writePipe = Partitioned.write[IO, Window, Key, Value](sinkFun, saveWindow(windowsRef))
        _ <- wkvStream[IO].filter(_.isInstanceOf[Record.Data[IO, _, _]]).through(writePipe).flatMap(_.sink).compile.drain.timeout(1000.millis).attempt
        windowsResult <- windowsRef.get

      } yield windowsResult

      val result = action.unsafeRunSync()

      result must beEmpty
    }

    "emit exactly one window in one KeyedEnqueue" in {
      implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

      val action = for {
        windowedRef <- Ref.of[IO, WindowedKV](Map.empty)
        windowsRef <- Ref.of[IO, List[Window]](Nil)
        sinkFun = PartitionedSpec.getSink(windowedRef) _
        writePipe = Partitioned.write[IO, Window, Key, Value](sinkFun, saveWindow(windowsRef))
        _ <- wkvStream[IO].through(writePipe).map(_.sink).head.flatten.compile.drain
        windowed <- windowedRef.get
        windowsResult <- windowsRef.get
      } yield (windowsResult, windowed)

      val (windows, result) = action.unsafeRunSync()

      windows must beEqualTo(List(0))
      removeKeys(result) must beEqualTo(Map(0 -> List(0,1,2,3,4)))
    }

    "execute checkpoint after end of window is reached" in {
      implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

      def mkCheckpoint(checkpointRef: Ref[IO, Int])(record: Record[IO, Window, Value]) =
        record match {
          case Record.EndWindow(_, _, _) => None
          case Record.Data(_, _, _) => checkpointRef.update(_ + 1).some
        }

      val action = for {
        sinkRef <- Ref.of[IO, WindowedKV](Map.empty)
        windowsRef <- Ref.of[IO, List[Window]](Nil)
        checkpointRef <- Ref.of[IO, Int](0)
        sinkFun = PartitionedSpec.getSink(sinkRef) _
        writePipe = Partitioned.write[IO, Window, Key, Value](sinkFun, saveWindow(windowsRef))
        _ <- wkvStream[IO](Partitioned.BufferSize, 1, mkCheckpoint(checkpointRef)(_)).filter(_.isInstanceOf[Record.Data[IO, _, _]]).take(5000).through(writePipe).flatMap(_.sink).compile.drain.attempt
        windowsResult <- windowsRef.get
        checkpointResult <- checkpointRef.get
      } yield (windowsResult, checkpointResult)

      val (windows, checkpoint) = action.unsafeRunSync()

      windows must beEmpty    // To make sure we haven't hit any EndWindow
      checkpoint must beEqualTo(1)
    }
  }
}

object PartitionedSpec {

  type Window = Int
  type Key = Int
  type Value = Int
  type WindowedKV = Map[Window, Map[Key, List[Value]]]

  def getSink(ref: Ref[IO, WindowedKV])(window: Window)(key: Key): Pipe[IO, Value, Unit] =
    _.evalMap { int =>
      ref.update { map =>
        map.get(window) match {
          case Some(keys) =>
            keys.get(key) match {
              case Some(list) => map.updated(window, keys.updated(key, int :: list))
              case None => map.updated(window, keys.updated(key, List(int)))
            }
          case None =>
            map.updated(window, Map(key -> List(int)))
        }
      }
    }


  def wkvStream[F[_]: Sync]: Stream[F, Record[F, Window, (Key, Value)]] =
    wkvStream[F](5, 3, noCheckpoint[F] _)

  /**
   * Stream of windowed key-value paris based on stream of natural numbers
   * Window starts at 0 and has wSize kv (only last one can be smaller)
   * Key is random number between 1 and kSize (inclusive)
   * Value starts at 0 and ever growing
   */
  def wkvStream[F[_]: Sync](wSize: Int = 5,
                            kSize: Int = 3,
                            mkCheckpoint: Record[F, Window, Value] => Option[F[Unit]] = noCheckpoint[F] _): Stream[F, Record[F, Window, (Key, Value)]] =
    Stream
      .iterate(0)(_ + wSize)
      .flatMap { i =>
        val window = Stream
          .iterate(i)(_ + 1)
          .map(j => Record.Data[F, Int, Int](i, None, j))
          .take(wSize.toLong) ++ Stream(Record.EndWindow[F, Int, Int](i, i + wSize, Sync[F].unit))
        window.map {
          case r @ Record.EndWindow(window, next, _) =>
            Record.EndWindow[F, Window, Value](window, next, mkCheckpoint(r).getOrElse(Sync[F].unit))
          case r @ Record.Data(window, _, item) =>
            Record.Data[F, Window, Value](window, mkCheckpoint(r), item)
        }
      }
      .evalMap { record => random[F](1, kSize).map(k => record.map(v => (k, v))) }

  def random[F[_]: Sync](start: Int, end: Int): F[Int] =
    Sync[F].delay(start + scala.util.Random.nextInt((end - start) + 1))

  /** All lists of values are continous lists of natural numbers */
  def isFullWindow(map: Map[Key, List[Value]]): Boolean = {
    val list = map
      .flatMap { case (_, list) => list }
      .toList
      .sorted

    val pattern = (list.head to list.last).toList
    list == pattern
  }

  def removeKeys(windowed: WindowedKV): Map[Window, List[Value]] =
    windowed.map { case (window, keyed) => (window, keyed.values.flatten.toList.sorted) }

  def saveWindow(windows: Ref[IO, List[Window]])(w: Window): IO[Unit] =
    windows.update(ws => w :: ws)

  def noCheckpoint[F[_]](record: Record[F, Window, Value]): Option[F[Unit]] = {
    val _ = record
    None
  }
}
