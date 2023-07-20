/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.metrics

import cats.implicits._

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8
import cats.effect.{Async, Resource, Sync}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.Config.MetricsReporters

/**
 * Reports metrics to a StatsD server over UDP
 *
 * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the
 * metric, e.g: `snowplow.transformer.good.count:20000|g|#app_id:12345,env:prod`
 */
object StatsDReporter {

  /**
   * A reporter which sends metrics from the registry to the StatsD server.
   *
   * The stream calls `InetAddress.getByName` each time there is a new batch of metrics. This allows
   * the run-time to resolve the address to a new IP address, in case DNS records change. This is
   * necessary in dynamic container environments (Kubernetes) where the statsd server could get
   * restarted at a new IP address.
   *
   * Note, InetAddress caches name resolutions, (see
   * https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/InetAddress.html) so
   * there could be a delay in following a DNS record change. For the Docker image we release the
   * cache time is 30 seconds.
   */
  def make[F[_]: Async](
    config: MetricsReporters.StatsD
  ): Resource[F, Reporter[F]] =
    Resource.fromAutoCloseable(Sync[F].delay(new DatagramSocket)).map(impl[F](config, _))

  private def impl[F[_]: Async](
    config: MetricsReporters.StatsD,
    socket: DatagramSocket
  ): Reporter[F] =
    new Reporter[F] {
      def report(snapshot: Metrics.MetricSnapshot): F[Unit] =
        (for {
          inetAddr <- Async[F].blocking(InetAddress.getByName(config.hostname))
          _ <- serializedMetrics(snapshot, config).traverse_(sendMetric[F](socket, inetAddr, config.port))
        } yield ()).handleErrorWith { t =>
          for {
            logger <- Slf4jLogger.create[F]
            _ <- Sync[F].delay(logger.error(t)("Caught exception sending metrics"))
          } yield ()
        }
    }

  type KeyValueMetric = (String, String)

  private def serializedMetrics(snapshot: Metrics.MetricSnapshot, config: MetricsReporters.StatsD): List[String] =
    keyValues(snapshot).map(statsDFormat(config))

  private def keyValues(snapshot: Metrics.MetricSnapshot): List[KeyValueMetric] =
    List(
      Metrics.goodCounterName -> snapshot.goodCount.toString,
      Metrics.badCounterName -> snapshot.badCount.toString
    )

  private def sendMetric[F[_]: Sync](
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    m: String
  ): F[Unit] = {
    val bytes = m.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    Sync[F].blocking(socket.send(packet))
  }

  private def statsDFormat(config: MetricsReporters.StatsD)(metric: KeyValueMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    s"${Metrics.normalizeMetric(config.prefix, metric._1)}:${metric._2}|c|#$tagStr"
  }
}
