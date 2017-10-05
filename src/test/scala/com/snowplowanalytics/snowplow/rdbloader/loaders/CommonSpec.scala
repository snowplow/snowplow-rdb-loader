/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader
package loaders

import cats._
import org.specs2.Specification

// This project
import S3.Key
import config.{ Step, StorageTarget }

class CommonSpec extends Specification { def is = s2"""
  Check that SSH tunnel gets open and closed if necessary $e1
  """

  def e1 = {
    val expected = List(
      "EC2 PROPERTY snowplow.redshift.key key", // Retrieve key
      "SSH TUNNEL ESTABLISH",                   // Open
      "LIST s3://snowplow-acme-storage/shredded/good/", "SLEEP 15000", "LIST s3://snowplow-acme-storage/shredded/good/",
      "BEGIN", "COPY", "INSERT", "COMMIT", "BEGIN", "ANALYZE", "COMMIT",
      "SSH TUNNEL CLOSE")                       // Close

    val actions = collection.mutable.ListBuffer.empty[String]

    // Inputs
    val key = StorageTarget.EncryptedConfig(StorageTarget.ParameterStoreConfig("snowplow.redshift.key"))
    val TunnelInput = StorageTarget.TunnelConfig(
      StorageTarget.BastionConfig("bastion.acme.com", 23, "bastion-user", None, Some(key)),
      15151,
      StorageTarget.DestinationConfig("10.0.0.17", 5433))
    val target = StorageTarget.RedshiftConfig(
      None,
      "test-redsfhit-target",
      "localhost",
      "snowplowdb",
      15151,
      StorageTarget.Disable,
      "arn:aws:iam::719197435995:role/RedshiftLoadRole",
      "update",
      "snowplow-loader",
      "Supersecret1",
      100,
      1000L,
      Some(TunnelInput))

    def interpreter: LoaderA ~> Id = new (LoaderA ~> Id) {
      def apply[A](effect: LoaderA[A]): Id[A] = {
        effect match {
          case LoaderA.ExecuteQuery(query) =>
            actions.append(query.split(" ").head.trim)
            Right(1L)

          case LoaderA.GetEc2Property(name) =>
            val value = "EC2 PROPERTY " ++ name ++ " key"
            actions.append(value)
            Right(value)

          case LoaderA.EstablishTunnel(Security.Tunnel(TunnelInput, Security.Identity(None, Some(_)))) =>
            actions.append("SSH TUNNEL ESTABLISH")
            Right(())

          case LoaderA.ListS3(bucket) =>
            actions.append(s"LIST $bucket")
            Right(List(Key.coerce(bucket ++ "run=2017-10-10-10-30-30/atomic-events/part-0001")))

          case LoaderA.Sleep(time) =>
            actions.append(s"SLEEP $time")

          case LoaderA.CloseTunnel() =>
            actions.append(s"SSH TUNNEL CLOSE")
            Right(())

          case action =>
            throw new RuntimeException(s"Unexpected Action [$action]")
        }
      }
    }

    val cliConfig = config.CliConfig(SpecHelpers.validConfig, target, Step.defaultSteps, None, None, false)
    val state = Common.load(cliConfig)
    val action = state.value.run(List.empty[Step])
    val (steps, result) = action.foldMap(interpreter)

    val transactionsExpectation = actions.toList must beEqualTo(expected)
    val resultExpectation = result must beRight
    val stepsExpectation = steps must beEqualTo(List(Step.Discover, Step.Load, Step.Analyze).reverse)
    transactionsExpectation.and(resultExpectation).and(stepsExpectation)
  }

}
