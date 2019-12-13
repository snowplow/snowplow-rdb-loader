/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
    val decline          = "0.6.2"
    val igluClient       = "0.6.0"
    val igluCore         = "0.5.1"
    val scalaTracker     = "0.6.1"
    val circeYaml        = "0.9.0"
    val circe            = "0.11.1"
    val circeOptics      = "0.11.0"
    val cats             = "1.6.1"
    val manifest         = "0.2.0"
    val fs2              = "1.0.4"

    // Scala (Shredder)
    val analyticsSdk     = "0.4.2"
    val spark            = "2.3.2"
    val eventsManifest   = "0.2.0"
    val schemaDdl        = "0.10.0"

    // Java (Loader)
    val postgres         = "42.0.0"
    val redshift         = "1.2.36.1060"
    val aws              = "1.11.319"
    val jSch             = "0.1.54"

    // Scala (test only)
    val specs2           = "4.0.4"
    val scalaCheck       = "1.12.6"
  }


  val resolutionRepos = Seq(
    // Redshift native driver
    "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
    // Speed-up build
    "snowplow" at "https://snowplow.bintray.com/snowplow-maven"
  )

  // Scala (Loader)
  val decline           = "com.monovore"          %% "decline"                           % V.decline
  val igluClient        = "com.snowplowanalytics" %% "iglu-scala-client"                 % V.igluClient
  val scalaTracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker-core"       % V.scalaTracker
  val scalaTrackerEmit  = "com.snowplowanalytics" %% "snowplow-scala-tracker-emitter-id" % V.scalaTracker
  val manifest          = "com.snowplowanalytics" %% "snowplow-processing-manifest"      % V.manifest
  val igluCore          = "com.snowplowanalytics" %% "iglu-core"                         % V.igluCore
  val igluCoreCirce     = "com.snowplowanalytics" %% "iglu-core-circe"                   % V.igluCore
  val cats              = "org.typelevel"         %% "cats"                              % V.cats
  val catsFree          = "org.typelevel"         %% "cats-free"                         % V.cats
  val circeCore         = "io.circe"              %% "circe-core"                        % V.circe
  val circeGeneric      = "io.circe"              %% "circe-generic"                     % V.circe
  val circeGenericExtra = "io.circe"              %% "circe-generic-extras"              % V.circe
  val circeYaml         = "io.circe"              %% "circe-yaml"                        % V.circeYaml
  val fs2               = "co.fs2"                %% "fs2-core"                          % V.fs2

  // Scala (Shredder)
  val analyticsSdk      = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val eventsManifest    = "com.snowplowanalytics" %% "snowplow-events-manifest"     % V.eventsManifest
  val schemaDdl         = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val circeJawn         = "io.circe"              %% "circe-jawn"                   % V.circe
  val circeLiteral      = "io.circe"              %% "circe-literal"                % V.circe
  val circeOptics       = "io.circe"              %% "circe-optics"                 % V.circeOptics     % "test"
  val sparkCore         = "org.apache.spark"      %% "spark-core"                   % V.spark           % "provided"
  val sparkSQL          = "org.apache.spark"      %% "spark-sql"                    % V.spark           % "provided"

  // Java (Loader)
  val postgres          = "org.postgresql"        % "postgresql"                % V.postgres
  val redshift          = "com.amazon.redshift"   % "redshift-jdbc42-no-awssdk" % V.redshift
  val redshiftSdk       = "com.amazonaws"         % "aws-java-sdk-redshift"     % V.aws
  val s3                = "com.amazonaws"         % "aws-java-sdk-s3"           % V.aws
  val ssm               = "com.amazonaws"         % "aws-java-sdk-ssm"          % V.aws
  val jSch              = "com.jcraft"            % "jsch"                      % V.jSch

  // Java (Shredder)
  val dynamodb          = "com.amazonaws"         % "aws-java-sdk-dynamodb"     % V.aws

  // Scala (test only)
  val specs2            = "org.specs2"            %% "specs2-core"             % V.specs2         % "test"
  val specs2ScalaCheck  = "org.specs2"            %% "specs2-scalacheck"       % V.specs2         % "test"
  val scalaCheck        = "org.scalacheck"        %% "scalacheck"              % V.scalaCheck     % "test"
}
