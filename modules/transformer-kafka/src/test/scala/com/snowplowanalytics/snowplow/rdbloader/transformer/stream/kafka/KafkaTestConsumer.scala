/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.azure.KafkaConsumer
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.Queue

object KafkaTestConsumer extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    KafkaConsumer
      .consumer[IO](
        bootstrapServers = "PLACEHOLDER-namespace.servicebus.windows.net:9093",
        topicName = "PLACEHOLDER",
        consumerConf = Map(
          "security.protocol" -> "SASL_SSL",
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"PLACEHOLDER\";",
          "group.id" -> "test-consumer",
          "enable.auto.commit" -> "true",
          "auto.offset.reset" -> "latest"
        )
      )
      .use(printConsumedMessages)
      .as(ExitCode.Success)

  private def printConsumedMessages(consumer: Queue.Consumer[IO]) =
    consumer.read
      .evalMap(message => IO(println(message.content)))
      .compile
      .drain
}
