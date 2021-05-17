package com.snowplowanalytics.snowplow.rdbloader.test

import cats.implicits._

import com.snowplowanalytics.snowplow.rdbloader.LoaderError
import com.snowplowanalytics.snowplow.rdbloader.dsl.Logging

case class PureLogging(print: String => Pure[Unit])

object PureLogging {
  val init: PureLogging = PureLogging(print)
  val noPrint: PureLogging = init.copy(print = PureLogging.noop)
  def withPredicate(p: String => Boolean): PureLogging =
    init.copy(print = s => if (p(s)) PureLogging.print(s) else PureLogging.noop(s) )

  def interpreter(results: PureLogging): Logging[Pure] = new Logging[Pure] {
    val sentry = None

    def track(result: Either[LoaderError, Unit]): Pure[Unit] =
      Pure.pure(())
    def info(message: String): Pure[Unit] =
      results.print(message)
    def error(message: String): Pure[Unit] =
      results.print(message)
    def trackException(e: Throwable): Pure[Unit] =
      results.print(s"EXCEPTION ${e.getMessage}")
  }

  def print(message: String): Pure[Unit] =
    Pure.modify(_.log(message))

  def noop(message: String): Pure[Unit] =
    Pure.pure(message).void
}
