/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.dsl.metrics

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8
import cats.implicits._
import cats.effect.Sync
import cats.effect.kernel.Resource
import com.snowplowanalytics.snowplow.rdbloader.config.Config

object StatsDReporter {

  def build[F[_]: Sync](statsDConfig: Option[Config.StatsD]): Reporter[F] =
    statsDConfig match {
      case Some(config) =>
        new Reporter[F] {
          def report(metrics: List[Metrics.KVMetric]): F[Unit] = {
            val formatted = metrics.map(statsDFormat(config))
            mkSocket[F].use { socket =>
              for {
                ip <- Sync[F].blocking(InetAddress.getByName(config.hostname))
                _ <- formatted.traverse_(sendMetric[F](socket, ip, config.port))
              } yield ()
            }
          }
        }
      case None =>
        Reporter.noop[F]
    }

  private def mkSocket[F[_]: Sync]: Resource[F, DatagramSocket] =
    Resource.fromAutoCloseable(Sync[F].delay(new DatagramSocket))

  private def sendMetric[F[_]: Sync](
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    metric: String
  ): F[Unit] = {
    val bytes = metric.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    Sync[F].blocking(socket.send(packet))
  }

  private def statsDFormat(config: Config.StatsD)(metric: Metrics.KVMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix.getOrElse(Config.MetricsDefaultPrefix).stripSuffix(".")
    s"${prefix}.${metric.key}:${metric.value}|${metric.metricType.render}|#$tagStr".stripPrefix(".")
  }
}
