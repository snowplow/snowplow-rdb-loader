/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.Path

import scala.concurrent.duration.{FiniteDuration, TimeUnit}
import cats.data.{EitherT, State}
import cats.effect.{Clock, Timer}
import cats.implicits._
import io.circe.literal._
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DSchemaList}
import com.snowplowanalytics.snowplow.rdbloader.utils.S3.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.db.Decoder
import com.snowplowanalytics.snowplow.rdbloader.db.Entities.{Columns, TableState}
import com.snowplowanalytics.snowplow.rdbloader.dsl.{AWS, Cache, Iglu, JDBC, Logging}
import com.snowplowanalytics.snowplow.rdbloader.loaders.Common.SqlString
import com.snowplowanalytics.snowplow.rdbloader.utils.S3


object TestInterpreter {

  case class TestState(genericLog: List[String], cache: Map[String, Option[S3.Key]]) {

    def getLog: List[String] = genericLog.reverse.map(trim)

    def log(message: String): TestState =
      TestState(message :: genericLog, cache)

    def time: Long =
      (genericLog.length + cache.size).toLong

    def cachePut(key: String, value: Option[S3.Key]): TestState =
      TestState(genericLog, cache ++ Map(key -> value))
  }

  object TestState {
    val init: TestState =
      TestState(List.empty[String], Map.empty[String, Option[S3.Key]])
  }

  type Test[A] = State[TestState, A]

  object Test {
    def apply[A](f: TestState => (TestState, A)): Test[A] = State(f)
    def liftWith[I, A](f: I => A)(a: I): Test[A] = State { s: TestState => (s, f(a)) }
    def pure[A](a: A): Test[A] = State.pure[TestState, A](a)
  }

  implicit class StateOps[A](st: Test[Either[LoaderError, A]]) {
    def toAction: LoaderAction[Test, A] = LoaderAction(st)
  }

  def testClock: Clock[Test] = new Clock[Test] {
    def realTime(unit: TimeUnit): Test[Long] =
      State { log: TestState => (log.log("TICK REALTIME"), log.time) }

    def monotonic(unit: TimeUnit): Test[Long] =
      State { log: TestState => (log.log("TICK MONOTONIC"), log.time) }
  }

  case class JDBCResults(executeQuery: SqlString => Decoder[Any] => LoaderAction[Test, Any])

  object JDBCResults {
    val init: JDBCResults = {
      def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Test, A] = {
        val result = ev.name match {
          case "TableState" => TableState(SchemaKey("com.acme", "some_context", "jsonschema", SchemaVer.Full(2,0,0)))
          case "Boolean" => false
          case "Columns" => Columns(List("some_column"))
        }
        val state = State { log: TestState => (log.log(query), result.asInstanceOf[A].asRight[LoaderError]) }
        state.toAction
      }

      JDBCResults(q => e => executeQuery(q)(e))
    }
  }

  def stateJdbcInterpreter(results: JDBCResults): JDBC[Test] = new JDBC[Test] {
    def executeUpdate(sql: SqlString): LoaderAction[Test, Long] = {
      val action = State { s: TestState => (s.log(sql), 1L.asRight[LoaderError]) }
      LoaderAction(action)
    }

    def executeQuery[A](query: SqlString)(implicit ev: Decoder[A]): LoaderAction[Test, A] =
      results.executeQuery.asInstanceOf[SqlString => Decoder[A] => LoaderAction[Test, A]](query)(ev)
  }


  case class ControlResults(print: String => Test[Unit])

  object ControlResults {
    def print(message: String): Test[Unit] =
      State.modify[TestState](_.log(message))

    def noop(message: String): Test[Unit] =
      State.modify[TestState](identity).void

    def init: ControlResults = ControlResults(print)
  }

  def stateControlInterpreter(results: ControlResults): Logging[Test] = new Logging[Test] {
    def getLastCopyStatements: Test[String] =
      Test.pure("No COPY in the test")
    def track(result: Either[LoaderError, Unit]): Test[Unit] =
      Test.pure(())
    def dump(key: Key)(implicit S: AWS[Test]): Test[Either[String, Key]] =
      Test.pure(key.asRight)
    def print(message: String): Test[Unit] =
      results.print(message)
  }

  def stateIgluInterpreter: Iglu[Test] = new Iglu[Test] {
    def getSchemas(vendor: String, name: String, model: Int): Test[Either[LoaderError, DSchemaList]] =
      SchemaList
        .parseStrings(List(s"iglu:$vendor/$name/jsonschema/$model-0-0"))
        .map { x => DSchemaList.fromSchemaList(x, TestInterpreter.fetch).value }
        .sequence[Test, Either[String, DSchemaList]]
        .map { e => e.flatten.leftMap { x => LoaderError.LoaderLocalError(x)} }
  }

  def stateTimerInterpreter: Timer[Test] = new Timer[Test] {
    def clock: Clock[Test] = testClock
    def sleep(duration: FiniteDuration): Test[Unit] =
      State { log: TestState => (log.log(s"SLEEP $duration"), ()) }
  }

  case class AWSResults(listS3: Folder => Test[Either[LoaderError, List[S3.BlobObject]]], keyExists: Key => Boolean)

  object AWSResults {
    val init: AWSResults = AWSResults(_ => State.pure(List.empty[S3.BlobObject].asRight), _ => false)
  }

  def stateAwsInterpreter(results: AWSResults): AWS[Test] = new AWS[Test] {
    def listS3(bucket: Folder): Test[Either[LoaderError, List[S3.BlobObject]]] =
      results.listS3(bucket).flatMap { list =>
        State.modify[TestState](s => s.log(s"LIST $bucket")).as(list)
      }

    def keyExists(key: Key): Test[Boolean] =
      State.pure(results.keyExists(key))

    def downloadData(source: Folder, dest: Path): LoaderAction[Test, List[Path]] =
      LoaderAction.liftF(Test.pure(List.empty[Path]))

    def putObject(key: Key, data: String): LoaderAction[Test, Unit] =
      LoaderAction.liftF(Test.pure(()))

    def getEc2Property(name: String): Test[Array[Byte]] =
      Test.pure(Array.empty[Byte])
  }

  def stateCacheInterpreter: Cache[Test] = new Cache[Test] {
    def putCache(key: String, value: Option[S3.Key]): Test[Unit] =
      State { log: TestState => (log.cachePut(key, value), ()) }

    def getCache(key: String): Test[Option[Option[S3.Key]]] =
      State { log: TestState => (log.log(s"GET $key"), log.cache.get(key)) }
  }

  private def fetch(key: SchemaKey): EitherT[Test, String, IgluSchema] = {
    val state = State[TestState, IgluSchema] { log =>
      val result = Schema.parse(json"""{}""").getOrElse(throw new RuntimeException("Not a valid JSON schema"))
      val schema = SelfDescribingSchema(SchemaMap(key), result)
      (log.log(s"Fetch ${key.toSchemaUri}"), schema)
    }
    EitherT.liftF(state)
  }

  private def trim(s: String): String =
    s.trim.replaceAll("\\s+", " ").replace("\n", " ")

}

