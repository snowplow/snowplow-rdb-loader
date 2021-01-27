package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.common.S3

/**
 * Intermediate state and final result of every effectful test spec
 * Used as a core of [[Pure]] effect
 * @param genericLog list of strings that every effect can add
 * @param cache JSONPaths cache
 */
case class TestState(genericLog: List[String], cache: Map[String, Option[S3.Key]]) {

  def getLog: List[String] = getLogUntrimmed.map(TestState.trim)
  def getLogUntrimmed: List[String] = genericLog.reverse

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

  def run[A](action: LoaderAction[Pure, A]) =
    action.value.value.run(init).value

  private def trim(s: String): String =
    s.trim.replaceAll("\\s+", " ").replace("\n", " ")
}
