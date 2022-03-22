package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.sinks.generic

sealed trait Status

object Status {
  /**
   * Initial state, the window is being pulled out of source and
   * potentially being written to the destination
   */
  case object Active extends Status

  /**
   * Pulling has reached `EndWindow`, but writing hasn't
   * More than one `Sealed` window is a sign of lagging sink
   *
   * Only pulling can change status from Active to Sealed
   */
  case object Sealed extends Status

  /**
   * The window has been written to destination, completely closed
   * and can be GC-ed
   *
   * Only sink can change status from Sealed to Closed
   */
  case object Closed extends Status
}

