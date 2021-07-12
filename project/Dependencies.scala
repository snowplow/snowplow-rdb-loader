/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
import sbt._

object Dependencies {

  object V {
    // Scala (Loader)
    val decline          = "1.4.0"
    val igluClient       = "1.1.0"
    val igluCore         = "1.0.0"
    val badrows          = "2.1.0"
    val analyticsSdk     = "2.1.0"
    val pureconfig       = "0.16.0"
    val circe            = "0.14.1"
    val cats             = "2.2.0"
    val manifest         = "0.3.0"
    val fs2              = "2.5.6"
    val fs2Aws           = "3.0.11"
    val fs2Blobstore     = "0.7.3"
    val doobie           = "0.12.1"
    val monocle          = "2.0.3"
    val catsRetry        = "2.1.0"
    val log4cats         = "1.3.0"
    val http4s           = "0.21.21"
    val scalaTracker     = "1.0.0"

    // Scala (Shredder)
    val spark            = "3.0.1"
    val eventsManifest   = "0.3.0"
    val schemaDdl        = "0.14.0"

    // Java (Loader)
    val slf4j            = "1.7.32"
    val redshift         = "1.2.55.1083"
    val aws              = "1.11.1019"
    val aws2             = "2.16.23"
    val jSch             = "0.1.55"
    val sentry           = "1.7.30"

    // Scala (test only)
    val specs2           = "4.10.5"
    val catsTesting      = "0.5.3"
    val scalaCheck       = "1.14.3"
  }

  val resolutionRepos = Seq(
    // Redshift native driver
    ("redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release").withAllowInsecureProtocol(true)
  )

  // Scala (Common)
  val http4sCore        = "org.http4s"                 %% "http4s-core"               % V.http4s
  val http4sCirce       = "org.http4s"                 %% "http4s-circe"              % V.http4s

  // Scala (Loader)
  val decline           = "com.monovore"               %% "decline"                           % V.decline
  val igluClient        = "com.snowplowanalytics"      %% "iglu-scala-client"                 % V.igluClient
  val igluClientHttp4s  = "com.snowplowanalytics"      %% "iglu-scala-client-http4s"          % V.igluClient
  val badrows           = "com.snowplowanalytics"      %% "snowplow-badrows"                  % V.badrows
  val igluCoreCirce     = "com.snowplowanalytics"      %% "iglu-core-circe"                   % V.igluCore
  val cats              = "org.typelevel"              %% "cats"                              % V.cats
  val circeCore         = "io.circe"                   %% "circe-core"                        % V.circe
  val circeGeneric      = "io.circe"                   %% "circe-generic"                     % V.circe
  val circeGenericExtra = "io.circe"                   %% "circe-generic-extras"              % V.circe
  val pureconfig        = "com.github.pureconfig"      %% "pureconfig"                        % V.pureconfig
  val pureconfigCirce   = "com.github.pureconfig"      %% "pureconfig-circe"                  % V.pureconfig
  val fs2               = "co.fs2"                     %% "fs2-core"                          % V.fs2
  val fs2Aws            = "io.laserdisc"               %% "fs2-aws"                           % V.fs2Aws
  val fs2AwsSqs         = "io.laserdisc"               %% "fs2-aws-sqs"                       % V.fs2Aws
  val fs2Blobstore      = "com.github.fs2-blobstore"   %% "s3"                                % V.fs2Blobstore
  val doobie            = "org.tpolecat"               %% "doobie-core"                       % V.doobie
  val analyticsSdk      = "com.snowplowanalytics"      %% "snowplow-scala-analytics-sdk"      % V.analyticsSdk
  val monocle           = "com.github.julien-truffaut" %% "monocle-core"                      % V.monocle
  val monocleMacro      = "com.github.julien-truffaut" %% "monocle-macro"                     % V.monocle
  val catsRetry         = "com.github.cb372"           %% "cats-retry"                        % V.catsRetry
  val log4cats          = "org.typelevel"              %% "log4cats-slf4j"                    % V.log4cats
  val http4sClient      = "org.http4s"                 %% "http4s-blaze-client"               % V.http4s
  val scalaTracker      = "com.snowplowanalytics"      %% "snowplow-scala-tracker-core"       % V.scalaTracker
  val scalaTrackerEmit  = "com.snowplowanalytics"      %% "snowplow-scala-tracker-emitter-http4s" % V.scalaTracker

  // Scala (Shredder)
  val eventsManifest    = "com.snowplowanalytics" %% "snowplow-events-manifest"     % V.eventsManifest
  val schemaDdl         = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val circeJawn         = "io.circe"              %% "circe-jawn"                   % V.circe
  val circeLiteral      = "io.circe"              %% "circe-literal"                % V.circe
  val circeOptics       = "io.circe"              %% "circe-optics"                 % V.circe           % Test
  val sparkCore         = "org.apache.spark"      %% "spark-core"                   % V.spark           % Provided
  val sparkSQL          = "org.apache.spark"      %% "spark-sql"                    % V.spark           % Provided
  val fs2Io             = "co.fs2"                %% "fs2-io"                       % V.fs2

  // Java (Loader)
  val slf4j             = "org.slf4j"             % "slf4j-simple"              % V.slf4j
  val redshift          = "com.amazon.redshift"   % "redshift-jdbc42-no-awssdk" % V.redshift
  val jSch              = "com.jcraft"            % "jsch"                      % V.jSch
  val sentry            = "io.sentry"             % "sentry"                    % V.sentry

  // Java (Shredder)
  val dynamodb          = "com.amazonaws"         % "aws-java-sdk-dynamodb"     % V.aws
  val sqs               = "com.amazonaws"         % "aws-java-sdk-sqs"          % V.aws
  val redshiftSdk       = "com.amazonaws"         % "aws-java-sdk-redshift"     % V.aws
  val ssm               = "com.amazonaws"         % "aws-java-sdk-ssm"          % V.aws

  val aws2s3            = "software.amazon.awssdk" % "s3"                       % V.aws2
  val aws2sqs           = "software.amazon.awssdk" % "sqs"                      % V.aws2
  val aws2kinesis       = "software.amazon.awssdk" % "kinesis"                  % V.aws2

  // Scala (test only)
  val specs2            = "org.specs2"                 %% "specs2-core"                % V.specs2      % Test
  val specs2ScalaCheck  = "org.specs2"                 %% "specs2-scalacheck"          % V.specs2      % Test
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"                 % V.scalaCheck  % Test
  val catsTesting       = "com.codecommit"             %% "cats-effect-testing-specs2" % V.catsTesting % Test
}
