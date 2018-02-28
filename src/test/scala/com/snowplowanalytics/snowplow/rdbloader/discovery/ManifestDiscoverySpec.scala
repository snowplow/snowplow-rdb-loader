/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package discovery

import java.time.Instant
import java.util.UUID

import scala.util.Random.shuffle

import cats._
import cats.data.{State => _, _}
import cats.implicits._
import cats.effect.{ IO, Sync }

import com.snowplowanalytics.iglu.core.SelfDescribingData

import io.circe.Json

import fs2.Stream

import com.snowplowanalytics.manifest.ItemId
import com.snowplowanalytics.manifest.core._
import com.snowplowanalytics.manifest.core.ManifestError._

import com.snowplowanalytics.snowplow.rdbloader.LoaderError.{DiscoveryError, ManifestFailure}
import com.snowplowanalytics.snowplow.rdbloader.config.Semver
import com.snowplowanalytics.snowplow.rdbloader.interpreters.implementations.ManifestInterpreter.ManifestE

import org.specs2.Specification

import SpecHelpers._

class ManifestDiscoverySpec extends Specification { def is = s2"""
  Return successful empty list for empty manifest $e1
  Return successful full discovery without shredded types $e2
  Return combined failure for invalid base path and invalid shredded type $e3
  Return multiple successfully discovered shredded types $e4
  Return multiple successfully discovered discoveries $e5
  """

  val shredderApp13 = Application(Agent("snowplow-rdb-shredder", "0.13.0"), None)
  val loaderApp13 = Application(Agent("snowplow-rdb-loader", "0.13.0"), None)
  def newId = UUID.randomUUID()
  val time = Instant.now()
  val time10 = time.plusSeconds(10)
  val time20 = time.plusSeconds(20)

  def e1 = {
    val action = ManifestDiscovery.discover("test-storage", "us-east-1", None)
    val result = action.value.foldMap(ManifestDiscoverySpec.interpreter(Nil))
    result must beRight(List.empty[Item])
  }

  def e2 = {
    val base = S3.Folder.coerce("s3://folder")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val author = Author(Agent("snowplow-rdb-shredder", "0.13.0"), "0.1.0-rc")
    val records = List(
      Record(base, shredderApp13, id1, None, State.New, time, author, None),
      Record(base, shredderApp13, id2, Some(id1), State.Processing, time10, author, None),
      Record(base, shredderApp13, newId, Some(id2), State.Processed, time20, author, None)
    )
    val item = Item(NonEmptyList.fromListUnsafe(records))

    val action = ManifestDiscovery.discover("test-storage", "us-east-1", None)
    val result = action.value.foldMap(ManifestDiscoverySpec.interpreter(records))
    result must beRight(List(
      DataDiscovery(base, None, Nil, specificFolder = false, Some(item))
    ))
  }

  def e3 = {
    val payload = getPayload("[\"iglu:com.acme/event/jsonschema/0-0-1\"]")
    val author = Author(Agent("snowplow-rdb-shredder", "0.13.0"), "0.1.0-rc")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val records = List(
      Record("invalidFolder", shredderApp13, id1, None, State.New, time, author, None),
      Record("invalidFolder", shredderApp13, id2, Some(id1), State.Processing, time10, author, None),
      Record("invalidFolder", shredderApp13, newId, Some(id2), State.Processed, time20, author, payload)
    )

    val action = ManifestDiscovery.discover("test-storage", "us-east-1", None)
    val result = action.value.foldMap(ManifestDiscoverySpec.interpreter(records))
    result must beLeft.like {
      case DiscoveryError(List(ManifestFailure(Corrupted(Corruption.ParseError(error))))) =>
        error must endingWith("Key [iglu:com.acme/event/jsonschema/0-0-1] is invalid Iglu URI, Path [invalidFolder] is not valid base for shredded type. Bucket name must start with s3:// prefix")
    }
  }

  def e4 = {
    val time = Instant.now()
    val base1 = S3.Folder.coerce("s3://snowplow-enriched-archive/shredded/good/run=2018-01-12-03-10-30")
    val base2 = S3.Folder.coerce("s3://snowplow-enriched-archive/shredded/good/run=2018-01-12-03-20-30")
    val payload1 = getPayload("[\"iglu:com.acme/event/jsonschema/1-0-1\"]")
    val payload2 = getPayload("[\"iglu:com.acme/context/jsonschema/1-0-0\", \"iglu:com.acme/context/jsonschema/1-0-1\"]")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val id3 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00002")
    val author = Author(Agent("snowplow-rdb-shredder", "0.13.0"), "0.1.0-rc")

    val records = List(
      Record(base1, shredderApp13, newId, None, State.New, time, author, None),
      Record(base1, shredderApp13, id1, Some(id2), State.Processing, time10, author, None),
      Record(base1, shredderApp13, newId, Some(id1), State.Processed, time20, author, payload1),
      Record(base1, loaderApp13, id2, None, State.Processing, time.plusSeconds(30), author, None),
      Record(base1, loaderApp13, newId, Some(id2), State.Processed, time.plusSeconds(30), author, None),

      Record(base2, shredderApp13, newId, None, State.New, time, author, None),
      Record(base2, shredderApp13, id3, None, State.Processing, time.plusSeconds(50), author, None),
      Record(base2, shredderApp13, newId, Some(id3), State.Processed, time.plusSeconds(60), author, payload2)
    )

    val item = Item(NonEmptyList.fromListUnsafe(records.slice(5, 8)))

    val action = ManifestDiscovery.discover("id", "us-east-1", None)
    val result = action.value.foldMap(ManifestDiscoverySpec.interpreter(records))
    result must beRight(List(
      DataDiscovery(base2, None, List(
        ShreddedType(
          ShreddedType.Info(base2, "com.acme", "context", 1, Semver(0,13,0)),
          S3.Key.coerce("s3://jsonpaths-assets/com.acme/context_1.json")
        )
      ), specificFolder = false, Some(item))
    ))
  }

  def e5 = {
    val time = Instant.now()
    val base1 = S3.Folder.coerce("s3://snowplow-enriched-archive/shredded/good/run=2018-01-12-03-10-30")
    val base2 = S3.Folder.coerce("s3://snowplow-enriched-archive/shredded/good/run=2018-01-12-03-20-30")
    val base3 = S3.Folder.coerce("s3://snowplow-enriched-archive/shredded/good/run=2018-01-12-03-30-30")
    val id1 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00000")
    val id2 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00001")
    val id3 = UUID.fromString("7c96c841-fc38-437d-bfec-4c1cd9b00002")
    val payload1 = getPayload("[\"iglu:com.acme/event/jsonschema/1-0-1\"]")
    val payload2 = getPayload("[\"iglu:com.acme/context/jsonschema/1-0-0\", \"iglu:com.acme/context/jsonschema/1-0-1\"]")
    val author = Author(Agent("snowplow-rdb-shredder", "0.13.0"), "0.1.0-rc")
    val records = List(
      Record(base1, shredderApp13, newId, None, State.New, time, author, None),
      Record(base1, shredderApp13, id1, None, State.Processing, time.plusSeconds(10), author, None),
      Record(base1, shredderApp13, newId, Some(id1), State.Processed, time.plusSeconds(20), author, payload1),

      Record(base2, shredderApp13, newId, None, State.New, time, author, None),
      Record(base2, shredderApp13, id2, None, State.Processing, time.plusSeconds(10), author, None),
      Record(base2, shredderApp13, newId, Some(id2), State.Processed, time.plusSeconds(20), author, payload1),

      Record(base3, shredderApp13, newId, None, State.New, time, author, None),
      Record(base3, shredderApp13, id3, None, State.Processing, time.plusSeconds(50), author, None),
      Record(base3, shredderApp13, newId, Some(id3), State.Processed, time.plusSeconds(60), author, payload2)
    )
    val item1 = Item(NonEmptyList.fromListUnsafe(records.slice(0, 3)))
    val item2 = Item(NonEmptyList.fromListUnsafe(records.slice(3, 6)))
    val item3 = Item(NonEmptyList.fromListUnsafe(records.slice(6, 9)))

    val expected = List(
      DataDiscovery(base1, None, List(
        ShreddedType(
          ShreddedType.Info(base1, "com.acme", "event", 1, Semver(0,13,0)),
          S3.Key.coerce("s3://jsonpaths-assets-other/com.acme/event_1.json")
        )
      ), specificFolder = false, Some(item1)),
      DataDiscovery(base2, None, List(
        ShreddedType(
          ShreddedType.Info(base2, "com.acme", "event", 1, Semver(0,13,0)),
          S3.Key.coerce("s3://jsonpaths-assets-other/com.acme/event_1.json")
        )
      ), specificFolder = false, Some(item2)),
      DataDiscovery(base3, None, List(
        ShreddedType(
          ShreddedType.Info(base3, "com.acme", "context", 1, Semver(0,13,0)),
          S3.Key.coerce("s3://jsonpaths-assets/com.acme/context_1.json")
        )
      ), specificFolder = false, Some(item3))
    )

    val action = ManifestDiscovery.discover("id", "us-east-1", None)
    val result = action.value.foldMap(ManifestDiscoverySpec.interpreter(records))
    result must beRight.like {
      case list => list must containTheSameElementsAs(expected)
    }
  }
}

object ManifestDiscoverySpec {

  def interpreter(records: List[Record]): LoaderA ~> Id = new (LoaderA ~> Id) {
    val manifest = ManifestDiscoverySpec.InMemoryManifest(records)
    def apply[A](effect: LoaderA[A]): Id[A] = {
      effect match {
        case LoaderA.ManifestDiscover(loader, shredder, predicate) =>
          manifest
            .getUnprocessed(manifest.query(Some(shredder), Some(loader)), predicate.getOrElse(_ => true))
            .leftMap(LoaderError.fromManifestError)
            .value
            .unsafeRunSync()

        case LoaderA.Get("com.acme/context_1.json") =>
          S3.Key.coerce("s3://jsonpaths-assets/com.acme/context_1.json").some.some

        case LoaderA.Get("com.acme/event_1.json") =>
          S3.Key.coerce("s3://jsonpaths-assets-other/com.acme/event_1.json").some.some

        case action =>
          throw new RuntimeException(s"Unexpected Action [$action]")
      }
    }
  }

  case class InMemoryManifest(records: List[Record]) extends ProcessingManifest[ManifestE](SpecHelpers.resolver) {

    val stateBuffer = collection.mutable.ListBuffer(records: _*)

    def mixed: List[Record] = shuffle(stateBuffer.toList)

    def getItem(id: ItemId): ManifestE[Option[Item]] = {
      val map = mixed.groupBy(_.itemId).map { case (i, r) => (i, Item(NonEmptyList.fromListUnsafe(r))) }
      EitherT.pure[IO, ManifestError](map.get(id))
    }

    def stream(implicit S: Sync[ManifestE]): Stream[ManifestE, Record] =
      Stream.emits(stateBuffer).covary[ManifestE]

    def fetch(processedBy: Option[Application], state: Option[State])
             (implicit F: ProcessingManifest.ManifestAction[ManifestE], S: Sync[ManifestE]): Stream[ManifestE, ItemId] =
      stream
        .filter(r => state.forall(_ == r.state))
        .filter(record => processedBy match {
          case Some(application) => inState(application, record, state)
          case None => true
        })
        .map(_.itemId)

    def put(id: ItemId,
            app: Application,
            previousId: Option[UUID],
            state: State,
            agent: Option[Agent],
            payload: Option[SelfDescribingData[Json]]): ManifestE[(UUID, Instant)] =
      EitherT.pure[IO, ManifestError] {
        val time = Instant.now()
        val a = Author(agent.getOrElse(app.agent), "0.1.0")
        val recordId = UUID.randomUUID()
        stateBuffer += Record(id, app, recordId, None, state, time, a, payload)
        (recordId, time)
      }

    // Copy-paste of `Item.inState`
    private final def inState(application: Application, record: Record, state: Option[State]): Boolean = {
      // Special check, allowing to mark existing Items *without* argument as processed
      // against application *with* arguments.
      val argsChecker: (String, Option[String]) => Boolean = {
        case (currentApp, instanceId) => currentApp == application.name && (application.instanceId match {
          case Some(_) if instanceId.isEmpty => true
          case Some(x) => instanceId.contains(x)
          case None => application.instanceId.isEmpty
        })
      }

      val app = record.application
      (state match {
        case Some(s) => s == record.state
        case None => true
      }) && argsChecker(app.name, app.instanceId)
    }
  }
}
