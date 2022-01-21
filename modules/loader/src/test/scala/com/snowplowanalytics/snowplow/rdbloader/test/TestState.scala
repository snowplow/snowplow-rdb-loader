package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.test.TestState._

/**
  * Intermediate state and final result of every effectful test spec
  * Used as a core of [[Pure]] effect
  * @param logOfF list of strings that every effect can add
  * @param cache JSONPaths cache
  */
case class TestState(
  logOfF: List[TestState.LogEntry.Message],
  cache: Map[String, Option[S3.Key]],
  private val stackOfC: List[TestState.LogEntry.Sql] = List.empty[TestState.LogEntry.Sql]
) {

  def getLog: List[LogEntry] = getLogUntrimmed.map {
    case LogEntry.Message(message) => LogEntry.Message(message)
    case LogEntry.Sql(statement)   => LogEntry.Sql(statement)
  }
  def getLogUntrimmed: List[LogEntry] = logOfF.reverse

  def log(message: String): TestState =
    TestState(LogEntry.Message(message) :: logOfF, cache, stackOfC)

  def sql(message: String): TestState =
    TestState(logOfF, cache, LogEntry.Sql(message) :: stackOfC)

  def transact: TestState =
    TestState(stackOfC.map(s => LogEntry.Message(s.content)) ++ logOfF, cache)

  def truncate: TestState = TestState(logOfF, cache)

  def time: Long =
    (logOfF.length + cache.size).toLong

  def cachePut(key: String, value: Option[S3.Key]): TestState =
    TestState(logOfF, cache ++ Map(key -> value), stackOfC)
}

object TestState {
  val init: TestState =
    TestState(
      List.empty[LogEntry.Message],
      Map.empty[String, Option[S3.Key]],
      List.empty[LogEntry.Sql]
    )

  def run[A](action: LoaderAction[Pure, A]) =
    action.value.value.run(init).value

  sealed trait LogEntry
  object LogEntry {
    case class Message(content: String) extends LogEntry
    case class Sql(content: String) extends LogEntry
  }
}
