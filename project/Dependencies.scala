/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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
    val scopt            = "3.6.0"
    val scalaz7          = "7.0.9"
    val igluClient       = "0.5.0"
    val igluCore         = "0.2.0"
    val scalaTracker     = "0.5.0"
    val circeYaml        = "0.7.0"
    val circe            = "0.9.3"
    val cats             = "1.1.0"
    val manifest         = "0.1.0-M3"

    // Scala (Shredder)
    val spark           = "2.2.0"
    val commonEnrich    = "0.27.0"

    // Java (Loader)
    val postgres         = "42.0.0"
    val redshift         = "1.2.8.1005"
    val aws              = "1.11.319"
    val jSch             = "0.1.54"

    // Scala (test only)
    val specs2           = "4.0.4"
    val scalaCheck       = "1.12.6"
  }


  val resolutionRepos = Seq(
    // For specs2
    "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
    // Redshift native driver
    "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
    // For Snowplow libs (SCE transient)
    "Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/",
    // For uaParser utils (SCE transient)
    "user-agent-parser repo" at "https://clojars.org/repo/"
  )

  // Scala (Loader)
  val scopt             = "com.github.scopt"      %% "scopt"                        % V.scopt
  val scalaz7           = "org.scalaz"            %% "scalaz-core"                  % V.scalaz7
  val igluClient        = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val scalaTracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker"       % V.scalaTracker
  val manifest          = "com.snowplowanalytics" %% "snowplow-processing-manifest" % V.manifest
  val igluCoreCirce     = "com.snowplowanalytics" %% "iglu-core-circe"              % V.igluCore
  val cats              = "org.typelevel"         %% "cats"                         % V.cats
  val catsFree          = "org.typelevel"         %% "cats-free"                    % V.cats
  val circeCore         = "io.circe"              %% "circe-core"                   % V.circe
  val circeYaml         = "io.circe"              %% "circe-yaml"                   % V.circeYaml
  val circeGeneric      = "io.circe"              %% "circe-generic"                % V.circe
  val circeGenericExtra = "io.circe"              %% "circe-generic-extras"         % V.circe

  // Scala (Shredder)
  val commonEnrich      = "com.snowplowanalytics" %% "snowplow-common-enrich" % V.commonEnrich
  val sparkCore         = "org.apache.spark"      %% "spark-core"             % V.spark           % "provided"
  val sparkSQL          = "org.apache.spark"      %% "spark-sql"              % V.spark           % "provided"

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
