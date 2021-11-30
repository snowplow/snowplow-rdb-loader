package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.core.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.core.db.{Statement => SqlStatement}

import com.snowplowanalytics.snowplow.rdbloader.test.TestState._

/**
  * Intermediate state and final result of every effectful test spec
  * Used as a core of [[Pure]] effect
  * @param genericLog list of strings that every effect can add
  * @param cache JSONPaths cache
  */
case class TestState(genericLog: List[TestState.LogEntry], cache: Map[String, Option[S3.Key]]) {

  def getLog: List[LogEntry] = getLogUntrimmed.map {
    case LogEntry.Message(message) => LogEntry.Message(trim(message))
    case LogEntry.Sql(statement)   => LogEntry.Sql(statement)
  }
  def getLogUntrimmed: List[LogEntry] = genericLog.reverse

  def log(message: String): TestState =
    TestState(LogEntry.Message(message) :: genericLog, cache)

  def log(statement: SqlStatement): TestState =
    TestState(LogEntry.Sql(statement) :: genericLog, cache)

  def time: Long =
    (genericLog.length + cache.size).toLong

  def cachePut(key: String, value: Option[S3.Key]): TestState =
    TestState(genericLog, cache ++ Map(key -> value))
}

object TestState {
  val init: TestState =
    TestState(List.empty[LogEntry], Map.empty[String, Option[S3.Key]])

  def run[A](action: LoaderAction[Pure, A]) =
    action.value.value.run(init).value

  private def trim(s: String): String =
    s.trim.replaceAll("\\s+", " ").replace("\n", " ")

  sealed trait LogEntry
  object LogEntry {
    case class Message(content: String) extends LogEntry
    case class Sql(content: SqlStatement) extends LogEntry
  }
}
