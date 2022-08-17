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
    val decline          = "2.1.0"
    val igluClient       = "1.2.0-M4"
    val igluCore         = "1.0.0"
    val badrows          = "2.1.0"
    val analyticsSdk     = "3.0.1"
    val pureconfig       = "0.16.0"
    val cron4sCirce      = "0.6.1"
    val circe            = "0.14.1"
    val cats             = "2.2.0"
    val catsEffect       = "2.5.4"
    val manifest         = "0.3.0"
    val fs2              = "2.5.6"
    val fs2Aws           = "3.0.11"
    val fs2Blobstore     = "0.7.3"
    val fs2Cron          = "0.5.0"
    val doobie           = "0.13.4"
    val monocle          = "2.0.3"
    val catsRetry        = "2.1.0"
    val log4cats         = "1.3.0"
    val http4s           = "0.21.33"
    val scalaTracker     = "1.0.0"

    val spark            = "3.1.2"
    val eventsManifest   = "0.3.0"
    val schemaDdl        = "0.15.0"
    val jacksonModule    = "2.13.2" // Override incompatible version in spark runtime
    val jacksonDatabind  = "2.13.2.2"
    val parquet4s        = "1.9.4"
    val hadoopClient     = "3.3.3"
    val parquetHadoop    = "1.12.3"

    val slf4j            = "1.7.32"
    val redshiftJdbc     = "1.2.55.1083"
    val snowflakeJdbc    = "3.13.9"
    val enumeratum       = "1.7.0"
    val aws              = "1.12.161"
    val aws2             = "2.17.59"
    val jSch             = "0.2.1"
    val sentry           = "1.7.30"
    val protobuf         = "3.16.1" // Fix CVE
    val commons          = "2.7"    // Fix CVE
    val kafkaClients     = "2.7.2"  // Fix CVE

    // Scala (test only)
    val specs2           = "4.10.5"
    val catsTesting      = "0.5.3"
    val scalaCheck       = "1.14.3"

    val betterMonadicFor = "0.3.1"
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
  val cron4sCirce       = "com.github.alonsodomin.cron4s" %% "cron4s-circe"                   % V.cron4sCirce
  val fs2               = "co.fs2"                     %% "fs2-core"                          % V.fs2
  val fs2Aws            = "io.laserdisc"               %% "fs2-aws"                           % V.fs2Aws
  val fs2AwsSqs         = "io.laserdisc"               %% "fs2-aws-sqs"                       % V.fs2Aws
  val fs2Blobstore      = "com.github.fs2-blobstore"   %% "s3"                                % V.fs2Blobstore
  val fs2Cron           = "eu.timepit"                 %% "fs2-cron-cron4s"                   % V.fs2Cron
  val doobie            = "org.tpolecat"               %% "doobie-core"                       % V.doobie
  val doobieHikari      = "org.tpolecat"               %% "doobie-hikari"                     % V.doobie
  val analyticsSdk      = "com.snowplowanalytics"      %% "snowplow-scala-analytics-sdk"      % V.analyticsSdk
  val monocle           = "com.github.julien-truffaut" %% "monocle-core"                      % V.monocle
  val monocleMacro      = "com.github.julien-truffaut" %% "monocle-macro"                     % V.monocle
  val catsRetry         = "com.github.cb372"           %% "cats-retry"                        % V.catsRetry
  val log4cats          = "org.typelevel"              %% "log4cats-slf4j"                    % V.log4cats
  val http4sClient      = "org.http4s"                 %% "http4s-blaze-client"               % V.http4s
  val scalaTracker      = "com.snowplowanalytics"      %% "snowplow-scala-tracker-core"       % V.scalaTracker
  val scalaTrackerEmit  = "com.snowplowanalytics"      %% "snowplow-scala-tracker-emitter-http4s" % V.scalaTracker

  // Scala (Shredder)
  val eventsManifest    = "com.snowplowanalytics"        %% "snowplow-events-manifest" % V.eventsManifest
  val schemaDdl         = "com.snowplowanalytics"        %% "schema-ddl"               % V.schemaDdl
  val circeJawn         = "io.circe"                     %% "circe-jawn"               % V.circe
  val circeLiteral      = "io.circe"                     %% "circe-literal"            % V.circe
  val circeOptics       = "io.circe"                     %% "circe-optics"             % V.circe           % Test
  val sparkCore         = "org.apache.spark"             %% "spark-core"               % V.spark           % Provided
  val sparkSQL          = "org.apache.spark"             %% "spark-sql"                % V.spark           % Provided
  val fs2Io             = "co.fs2"                       %% "fs2-io"                   % V.fs2

  val jacksonModule     = "com.fasterxml.jackson.module"     %% "jackson-module-scala"     % V.jacksonModule
  val jacksonDatabind   = "com.fasterxml.jackson.core"       %  "jackson-databind"         % V.jacksonDatabind
  val jacksonCbor       = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"   % V.jacksonModule
  val parquet4s         = "com.github.mjakubowski84"         %% "parquet4s-fs2"            % V.parquet4s
  val hadoop            = "org.apache.hadoop"                % "hadoop-client"             % V.hadoopClient
  val parquetHadoop     = "org.apache.parquet"               % "parquet-hadoop"            % V.parquetHadoop
  val hadoopAws         = ("org.apache.hadoop"               % "hadoop-aws"                % V.hadoopClient % Runtime)
    .exclude("com.amazonaws", "aws-java-sdk-bundle") // aws-java-sdk-core is already present in assembled jar

  // Java (Loader)
  val slf4j             = "org.slf4j"             % "slf4j-simple"              % V.slf4j
  val redshift          = "com.amazon.redshift"   % "redshift-jdbc42-no-awssdk" % V.redshiftJdbc
  val jSch              = "com.github.mwiede"     % "jsch"                      % V.jSch
  val sentry            = "io.sentry"             % "sentry"                    % V.sentry
  val snowflakeJdbc     = "net.snowflake"         % "snowflake-jdbc"            % V.snowflakeJdbc
  val enumeratum        = "com.beachape"          %% "enumeratum"               % V.enumeratum

  // Java (Shredder)
  val dynamodb          = "com.amazonaws"         % "aws-java-sdk-dynamodb"     % V.aws
  val sqs               = "com.amazonaws"         % "aws-java-sdk-sqs"          % V.aws
  val sns               = "com.amazonaws"         % "aws-java-sdk-sns"          % V.aws
  val redshiftSdk       = "com.amazonaws"         % "aws-java-sdk-redshift"     % V.aws
  val ssm               = "com.amazonaws"         % "aws-java-sdk-ssm"          % V.aws

  val aws2s3            = "software.amazon.awssdk" % "s3"                       % V.aws2
  val aws2sqs           = "software.amazon.awssdk" % "sqs"                      % V.aws2
  val aws2sns           = "software.amazon.awssdk" % "sns"                      % V.aws2
  val aws2kinesis       = "software.amazon.awssdk" % "kinesis"                  % V.aws2
  val aws2regions       = "software.amazon.awssdk" % "regions"                  % V.aws2
  val aws2sts           = "software.amazon.awssdk" % "sts"                      % V.aws2
  val protobuf          = "com.google.protobuf"    % "protobuf-java"            % V.protobuf
  val commons           = "commons-io"             % "commons-io"               % V.commons
  val kafkaClients      = "org.apache.kafka"       % "kafka-clients"            % V.kafkaClients

  // Scala (test only)
  val specs2            = "org.specs2"                 %% "specs2-core"                % V.specs2      % Test
  val specs2ScalaCheck  = "org.specs2"                 %% "specs2-scalacheck"          % V.specs2      % Test
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"                 % V.scalaCheck  % Test
  val catsTesting       = "com.codecommit"             %% "cats-effect-testing-specs2" % V.catsTesting % Test
  val catsEffectLaws    = "org.typelevel"              %% "cats-effect-laws"           % V.catsEffect  % Test

  // compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  val awsDependencies = Seq(
    aws2s3,
    aws2sqs,
    aws2sns,
    fs2,
    catsRetry,
  )

  val commonDependencies = Seq(
    decline,
    analyticsSdk,
    badrows,
    igluClient,
    circeGeneric,
    circeGenericExtra,
    circeLiteral,
    pureconfig,
    pureconfigCirce,
    cron4sCirce,
    schemaDdl,
    http4sCore,
    aws2regions,
    jacksonDatabind,
    specs2,
    monocle,
    monocleMacro,
  )

  val loaderDependencies = Seq(
    slf4j,
    ssm,
    aws2sts,
    dynamodb,
    jSch,
    sentry,
    scalaTracker,
    scalaTrackerEmit,
    fs2Blobstore,
    fs2Cron,
    http4sCirce,
    http4sClient,
    igluClientHttp4s,
    doobie,
    doobieHikari,
    catsRetry,
    log4cats,
    specs2,
    specs2ScalaCheck,
    scalaCheck,
    catsEffectLaws,
    catsTesting,
  )

  val redshiftDependencies = Seq(
    redshift,
    redshiftSdk
  )

  val snowflakeDependencies = Seq(
    enumeratum,
    snowflakeJdbc
  )

  val batchTransformerDependencies = Seq(
    sqs,
    sns,
    dynamodb,
    slf4j,
    sentry,
    eventsManifest,
    sparkCore,
    sparkSQL,
    jacksonModule,
    jacksonDatabind,
    jacksonCbor,
    circeOptics,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  )

  val transformerKinesisDependencies = Seq(
    dynamodb,
    slf4j,
    protobuf,
    commons,
    kafkaClients,
    log4cats,
    fs2Blobstore,
    fs2Io,
    fs2Aws,
    fs2AwsSqs,
    aws2kinesis,
    http4sClient,
    catsEffectLaws,
    circeOptics,
    parquet4s,
    hadoop,
    hadoopAws,
    parquetHadoop,
    scalaTracker,
    scalaTrackerEmit,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  )

  // exclusions
  val exclusions = Seq(
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  )

  val transformerKinesisExclusions = {
    exclusions ++ Seq(
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-api"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-client"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-mapreduce-client-jobclient"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-hdfs-client"),
      ExclusionRule(organization = "org.apache.hadoop.thirdparty", name = "hadoop-shaded-protobuf_3_7"),
    )
  }
}
