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
package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.db.Statement

import com.snowplowanalytics.snowplow.rdbloader.test.TestState._

/**
 * Intermediate state and final result of every effectful test spec Used as a core of [[Pure]]
 * effect
 * @param genericLog
 *   list of strings that every effect can add
 * @param cache
 *   JSONPaths cache
 */
case class TestState(genericLog: List[TestState.LogEntry], cache: Map[String, Option[BlobStorage.Key]]) {

  def getLog: List[LogEntry] = getLogUntrimmed.map {
    case LogEntry.Message(message) => LogEntry.Message(trim(message))
    case LogEntry.Sql(statement) => LogEntry.Sql(statement)
  }
  def getLogUntrimmed: List[LogEntry] = genericLog.reverse

  def log(message: String): TestState =
    TestState(LogEntry.Message(message) :: genericLog, cache)

  def log(statement: Statement): TestState =
    TestState(LogEntry.Sql(statement) :: genericLog, cache)

  def time: Long =
    (genericLog.length + cache.size).toLong

  def cachePut(key: String, value: Option[BlobStorage.Key]): TestState =
    TestState(genericLog, cache ++ Map(key -> value))
}

object TestState {
  val init: TestState =
    TestState(List.empty[LogEntry], Map.empty[String, Option[BlobStorage.Key]])

  def run[A](action: LoaderAction[Pure, A]) =
    action.value.value.run(init).value

  private def trim(s: String): String =
    s.trim.replaceAll("\\s+", " ").replace("\n", " ")

  sealed trait LogEntry
  object LogEntry {
    case class Message(content: String) extends LogEntry
    case class Sql(content: Statement) extends LogEntry {
      override def equals(obj: Any): Boolean =
        content match {
          case Statement.CreateTable(t) =>
            obj match {
              case Sql(Statement.CreateTable(f)) =>
                t.internals.sql == f.internals.sql
              case _ => false
            }
          case Statement.AlterTable(t) =>
            obj match {
              case Sql(Statement.AlterTable(f)) =>
                t.internals.sql == f.internals.sql
              case _ => false
            }
          case Statement.DdlFile(t) =>
            obj match {
              case Sql(Statement.DdlFile(f)) =>
                t.internals.sql == f.internals.sql
              case _ => false
            }
          case statement =>
            obj match {
              case Sql(other) => statement == other
              case _ => false
            }
        }
    }
  }
}
