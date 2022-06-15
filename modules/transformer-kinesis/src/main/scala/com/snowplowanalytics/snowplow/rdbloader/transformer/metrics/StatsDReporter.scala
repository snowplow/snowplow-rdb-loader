/*
 * Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.transformer.metrics

import cats.implicits._

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.{Blocker, ContextShift, Resource, Sync, Timer}

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis.Config.MetricsReporters

/**
 * Reports metrics to a StatsD server over UDP
 *
 * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the metric, e.g:
 * `snowplow.transformer.good.count:20000|g|#app_id:12345,env:prod`
 */
object StatsDReporter {

  /**
   * A reporter which sends metrics from the registry to the StatsD server.
   *
   * The stream calls `InetAddress.getByName` each time there is a new batch of metrics. This allows
   * the run-time to resolve the address to a new IP address, in case DNS records change.  This is
   * necessary in dynamic container environments (Kubernetes) where the statsd server could get
   * restarted at a new IP address.
   *
   * Note, InetAddress caches name resolutions, (see https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/InetAddress.html)
   * so there could be a delay in following a DNS record change.  For the Docker image we release
   * the cache time is 30 seconds.
   */
  def make[F[_]: Sync: ContextShift: Timer](
    blocker: Blocker,
    config: MetricsReporters.StatsD
  ): Resource[F, Reporter[F]] =
    Resource.fromAutoCloseable(Sync[F].delay(new DatagramSocket)).map(impl[F](blocker, config, _))

  private def impl[F[_]: Sync: ContextShift: Timer](
    blocker: Blocker,
    config: MetricsReporters.StatsD,
    socket: DatagramSocket
  ): Reporter[F] =
    new Reporter[F] {
      def report(snapshot: Metrics.MetricSnapshot): F[Unit] =
        (for {
          inetAddr <- blocker.delay(InetAddress.getByName(config.hostname))
          _ <- serializedMetrics(snapshot, config).traverse_(sendMetric[F](blocker, socket, inetAddr, config.port))
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
      Metrics.badCounterName -> snapshot.badCount.toString,
    )

  private def sendMetric[F[_]: ContextShift: Sync](
    blocker: Blocker,
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    m: String
  ): F[Unit] = {
    val bytes = m.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    blocker.delay(socket.send(packet))
  }

  private def statsDFormat(config: MetricsReporters.StatsD)(metric: KeyValueMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    s"${Metrics.normalizeMetric(config.prefix, metric._1)}:${metric._2}|c|#$tagStr"
  }
}
