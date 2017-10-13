/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.  */
import sbt._

object Dependencies {

  object V {
    // Scala (Loader)
    val scopt            = "3.6.0"
    val scalaz7          = "7.0.9"
    val json4s           = "3.2.11" // evicted by iglu-core with 3.3.0
    val igluClient       = "0.5.0"
    val igluCore         = "0.1.0"
    val scalaTracker     = "0.3.0"
    val circeYaml        = "0.6.1"
    val circe            = "0.8.0"
    val cats             = "0.9.0"

    // Scala (Shredder)
    val spark           = "2.2.0"
    val commonEnrich    = "0.27.0"

    // Java (Loader)
    val postgres         = "42.0.0"
    val redshift         = "1.2.8.1005"
    val aws              = "1.11.208"
    val jSch             = "0.1.54"

    // Java (Shredder)
    val dynamodb        = "1.11.98"

    // Scala (test only)
    val specs2           = "3.6-scalaz-7.0.7"
    val scalaCheck       = "1.12.6"
  }


  val resolutionRepos = Seq(
    // For specs2
    "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases",
    // Redshift native driver
    "redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release"
  )

  // Scala (Loader)
  val scopt             = "com.github.scopt"      %% "scopt"                  % V.scopt
  val scalaz7           = "org.scalaz"            %% "scalaz-core"            % V.scalaz7
  val json4s            = "org.json4s"            %% "json4s-jackson"         % V.json4s
  val igluClient        = "com.snowplowanalytics" %% "iglu-scala-client"      % V.igluClient
  val igluCore          = "com.snowplowanalytics" %% "iglu-core"              % V.igluCore     intransitive()
  val scalaTracker      = "com.snowplowanalytics" %% "snowplow-scala-tracker" % V.scalaTracker
  val cats              = "org.typelevel"         %% "cats"                   % V.cats
  val catsFree          = "org.typelevel"         %% "cats-free"              % V.cats
  val circeCore         = "io.circe"              %% "circe-core"             % V.circe
  val circeYaml         = "io.circe"              %% "circe-yaml"             % V.circeYaml
  val circeGeneric      = "io.circe"              %% "circe-generic"          % V.circe
  val circeGenericExtra = "io.circe"              %% "circe-generic-extras"   % V.circe

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
  val dynamodb          = "com.amazonaws"         % "aws-java-sdk-dynamodb"   % V.dynamodb

  // Scala (test only)
  val specs2            = "org.specs2"            %% "specs2-core"             % V.specs2         % "test"
  val specs2ScalaCheck  = "org.specs2"            %% "specs2-scalacheck"       % V.specs2         % "test"
  val scalaCheck        = "org.scalacheck"        %% "scalacheck"              % V.scalaCheck     % "test"
}
