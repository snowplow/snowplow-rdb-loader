/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
import sbt._

object Dependencies {

  object V {
    // Scala (Loader)
    val decline          = "2.4.1"
    val igluClient       = "3.0.0"
    val igluCore         = "1.1.1"
    val badrows          = "2.2.0"
    val analyticsSdk     = "3.1.0"
    val cron4sCirce      = "0.6.1"
    val circe            = "0.14.1"
    val circeConfig      = "0.10.0"
    val cats             = "2.2.0"
    val catsEffect       = "3.3.14"
    val manifest         = "0.3.0"
    val fs2              = "3.3.0"
    val fs2Aws           = "4.1.0"
    val fs2Blobstore     = "0.9.7"
    val fs2Cron          = "0.7.2"
    val fs2PubSub        = "0.21.0"
    val fs2Kafka         = "3.0.0"
    val secretManager    = "2.7.0"
    val gcpStorage       = "2.16.0"
    val azureIdentity    = "1.9.0"
    val azureKeyVault    = "4.6.2"
    val doobie           = "1.0.0-RC2"
    val monocle          = "2.0.3"
    val catsRetry        = "3.1.0"
    val log4cats         = "2.5.0"
    val http4s           = "0.23.17"
    val http4sBlaze     = "0.23.14" // this dep fell out of sync with http4s-core versioning - 0.23.14 is the last 0.X release.
    val scalaTracker     = "2.0.0"

    val spark            = "3.3.1"
    val eventsManifest   = "0.4.0"
    val schemaDdl        = "0.18.2"
    val jacksonModule    = "2.14.2" // Override incompatible version in spark runtime
    val jacksonDatabind  = "2.14.2"
    val jacksonMapper    = "1.9.14-atlassian-6" // Fix CVE
    val parquet4s        = "2.10.0"
    val hadoopClient     = "3.3.4"
    val hadoopGcpClient  = "hadoop3-2.2.5"
    val parquetHadoop    = "1.12.3"
    val jsonSmart        = "2.4.9" // Fix CVE
    val nimbusJose       = "9.31" // Fix CVE
    val snappyJava       = "1.1.10.4" // Fix CVE
    val commonsText      = "1.10.0" // Fix CVE
    val ivy              = "2.5.1" // Fix CVE

    val slf4j            = "2.0.3"
    val redshiftJdbc     = "1.2.55.1083"
    val snowflakeJdbc    = "3.13.30"
    val enumeratum       = "1.7.0"
    val aws              = "1.12.261"
    val aws2             = "2.21.33"
    val jSch             = "0.2.1"
    val sentry           = "1.7.30"
    val protobuf         = "3.21.7" // Fix CVE
    val kinesisClient    = "2.4.3"
    val nettyCodec       = "4.1.86.Final" // Fix CVE
    val jettison         = "1.5.4" // Fix CVE
    val reactorNetty     = "1.0.39" // Fix CVE
    val jettyHttp        = "9.4.53.v20231009" // Fix CVE
    val zookeeper        = "3.7.2" // Fix CVE

    // Scala (test only)
    val specs2           = "4.17.0"
    val catsTesting      = "1.5.0"
    val catsEffectTestkit = "3.4.5"
    val scalaCheck       = "1.17.0"
    val eventGenerator   = "0.4.0"

    val betterMonadicFor = "0.3.1"
  }

  val resolutionRepos = Seq(
    // Redshift native driver
    ("redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release").withAllowInsecureProtocol(true),
    ("Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true),
    ("Atlassian Maven repo" at "https://packages.atlassian.com/mvn/maven-3rdparty/")
  )

  // Scala (Common)
  val http4sCore        = "org.http4s"                 %% "http4s-core"               % V.http4s
  val http4sCirce       = "org.http4s"                 %% "http4s-circe"              % V.http4s
  val catsEffectKernel  = "org.typelevel"              %% "cats-effect-kernel"        % V.catsEffect

  // Scala (Loader)
  val decline           = "com.monovore"               %% "decline"                           % V.decline
  val igluClient        = "com.snowplowanalytics"      %% "iglu-scala-client"                 % V.igluClient
  val igluClientHttp4s  = "com.snowplowanalytics"      %% "iglu-scala-client-http4s"          % V.igluClient
  val badrows           = "com.snowplowanalytics"      %% "snowplow-badrows"                  % V.badrows
  val igluCoreCirce     = "com.snowplowanalytics"      %% "iglu-core-circe"                   % V.igluCore
  val cats              = "org.typelevel"              %% "cats"                              % V.cats
  val circeCore         = "io.circe"                   %% "circe-core"                        % V.circe
  val circeConfig       = "io.circe"                   %% "circe-config"                      % V.circeConfig
  val circeGeneric      = "io.circe"                   %% "circe-generic"                     % V.circe
  val circeGenericExtra = "io.circe"                   %% "circe-generic-extras"              % V.circe
  val cron4sCirce       = ("com.github.alonsodomin.cron4s" %% "cron4s-circe"                   % V.cron4sCirce)
    .exclude("io.circe", "circe-core_2.12") // cron4s-circe lacks circe 0.13 support
  val fs2               = "co.fs2"                     %% "fs2-core"                          % V.fs2
  val fs2Kafka          = "com.github.fd4s"            %% "fs2-kafka"                         % V.fs2Kafka
  val fs2Kinesis        = ("io.laserdisc"              %% "fs2-aws-kinesis"              % V.fs2Aws)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.kinesis", "amazon-kinesis-client")
  val fs2BlobstoreS3    = "com.github.fs2-blobstore"   %% "s3"                                % V.fs2Blobstore
  val fs2BlobstoreGCS   = "com.github.fs2-blobstore"   %% "gcs"                               % V.fs2Blobstore
  val fs2BlobstoreAzure = "com.github.fs2-blobstore"   %% "azure"                             % V.fs2Blobstore
  val azureIdentity     = "com.azure"                  % "azure-identity"                     % V.azureIdentity
  val azureKeyVault     = "com.azure"                  % "azure-security-keyvault-secrets"    % V.azureKeyVault
  val fs2Cron           = "eu.timepit"                 %% "fs2-cron-cron4s"                   % V.fs2Cron
  val fs2PubSub         = "com.permutive"              %% "fs2-google-pubsub-grpc"            % V.fs2PubSub
  val secretManager     = "com.google.cloud"           % "google-cloud-secretmanager"         % V.secretManager
  val gcpStorage        = "com.google.cloud"           % "google-cloud-storage"               % V.gcpStorage
  val doobie            = "org.tpolecat"               %% "doobie-core"                       % V.doobie
  val doobieHikari      = "org.tpolecat"               %% "doobie-hikari"                     % V.doobie
  val analyticsSdk      = "com.snowplowanalytics"      %% "snowplow-scala-analytics-sdk"      % V.analyticsSdk
  val monocle           = "com.github.julien-truffaut" %% "monocle-core"                      % V.monocle
  val monocleMacro      = "com.github.julien-truffaut" %% "monocle-macro"                     % V.monocle
  val catsRetry         = "com.github.cb372"           %% "cats-retry"                        % V.catsRetry
  val log4cats          = "org.typelevel"              %% "log4cats-slf4j"                    % V.log4cats
  val http4sClient      = "org.http4s"                 %% "http4s-blaze-client"               % V.http4sBlaze
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
  val jacksonMapper     = "org.codehaus.jackson"             % "jackson-mapper-asl"        % V.jacksonMapper
  val parquet4s         = "com.github.mjakubowski84"         %% "parquet4s-fs2"            % V.parquet4s
  val hadoopCommon      = ("org.apache.hadoop"               % "hadoop-common"             % V.hadoopClient)
    .exclude("com.jcraft", "jsch")
    .exclude("org.apache.zookeeper", "zookeeper")
  val hadoop            = "org.apache.hadoop"                % "hadoop-client"             % V.hadoopClient
  val parquetHadoop     = "org.apache.parquet"               % "parquet-hadoop"            % V.parquetHadoop
  val hadoopAws         = ("org.apache.hadoop"               % "hadoop-aws"                % V.hadoopClient % Runtime)
    .exclude("com.amazonaws", "aws-java-sdk-bundle") // aws-java-sdk-core is already present in assembled jar
  val hadoopGcp         = "com.google.cloud.bigdataoss"      % "gcs-connector"             % V.hadoopGcpClient % Runtime
  val jsonSmart         = "net.minidev"                      % "json-smart"                % V.jsonSmart
  val nimbusJose        = ("com.nimbusds"                    % "nimbus-jose-jwt"           % V.nimbusJose)
  val snappyJava        = "org.xerial.snappy"                % "snappy-java"               % V.snappyJava
  val hadoopAzure       = "org.apache.hadoop"                % "hadoop-azure"              % V.hadoopClient
  val kinesisClient     = ("software.amazon.kinesis"         %  "amazon-kinesis-client"    % V.kinesisClient)
                          .exclude("software.amazon.glue", "schema-registry-common")
                          .exclude("software.amazon.glue", "schema-registry-serde")
  val commonsText       = "org.apache.commons"               % "commons-text"              % V.commonsText
  val ivy               = "org.apache.ivy"                   % "ivy"                       % V.ivy

  // Java (Loader)
  val slf4jApi          = "org.slf4j"             % "slf4j-api"                 % V.slf4j
  val slf4jSimple       = "org.slf4j"             % "slf4j-simple"              % V.slf4j
  val redshift          = "com.amazon.redshift"   % "redshift-jdbc42-no-awssdk" % V.redshiftJdbc
  val jSch              = "com.github.mwiede"     % "jsch"                      % V.jSch
  val sentry            = "io.sentry"             % "sentry"                    % V.sentry
  val snowflakeJdbc     = "net.snowflake"         % "snowflake-jdbc"            % V.snowflakeJdbc
  val enumeratum        = "com.beachape"          %% "enumeratum"               % V.enumeratum
  val jettison          = "org.codehaus.jettison" % "jettison"                  % V.jettison
  val reactorNetty      = "io.projectreactor.netty" % "reactor-netty-http"      % V.reactorNetty
  val jettyHttp         = "org.eclipse.jetty" % "jetty-http" % V.jettyHttp

  // Java (Shredder)
  val dynamodb          = "com.amazonaws"         % "aws-java-sdk-dynamodb"     % V.aws
  val sqs               = "com.amazonaws"         % "aws-java-sdk-sqs"          % V.aws
  val sns               = "com.amazonaws"         % "aws-java-sdk-sns"          % V.aws
  val redshiftSdk       = "com.amazonaws"         % "aws-java-sdk-redshift"     % V.aws
  val ssm               = "com.amazonaws"         % "aws-java-sdk-ssm"          % V.aws
  val sts               = "com.amazonaws"         % "aws-java-sdk-sts"          % V.aws % Runtime
  val kinesis           = "com.amazonaws"         % "aws-java-sdk-kinesis"      % V.aws
  val cloudwatch        = "com.amazonaws"         % "aws-java-sdk-cloudwatch"   % V.aws

  val aws2s3            = "software.amazon.awssdk" % "s3"                       % V.aws2
  val aws2sqs           = "software.amazon.awssdk" % "sqs"                      % V.aws2
  val aws2sns           = "software.amazon.awssdk" % "sns"                      % V.aws2
  val aws2kinesis       = "software.amazon.awssdk" % "kinesis"                  % V.aws2
  val aws2regions       = "software.amazon.awssdk" % "regions"                  % V.aws2
  val aws2sts           = "software.amazon.awssdk" % "sts"                      % V.aws2        % Runtime
  val protobuf          = "com.google.protobuf"    % "protobuf-java"            % V.protobuf
  val nettyCodec        = "io.netty"               % "netty-codec"              % V.nettyCodec
  val zookeeper         = "org.apache.zookeeper"   % "zookeeper"                % V.zookeeper

  // Scala (test only)
  val specs2            = "org.specs2"                 %% "specs2-core"                   % V.specs2            % Test
  val specs2ScalaCheck  = "org.specs2"                 %% "specs2-scalacheck"             % V.specs2            % Test
  val scalaCheck        = "org.scalacheck"             %% "scalacheck"                    % V.scalaCheck        % Test
  val catsTesting       = "org.typelevel"              %% "cats-effect-testing-specs2"    % V.catsTesting       % Test
  val catsEffectTestkit = "org.typelevel"              %% "cats-effect-testkit"           % V.catsEffectTestkit % Test
  val catsEffectLaws    = "org.typelevel"              %% "cats-effect-laws"              % V.catsEffect        % Test
  val fs2BlobstoreCore  = "com.github.fs2-blobstore"   %% "core"                          % V.fs2Blobstore      % Test
  val eventGenerator    = "com.snowplowanalytics"      %% "snowplow-event-generator-core" % V.eventGenerator    % Test

  // compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  val awsDependencies = Seq(
    aws2s3,
    aws2sqs,
    aws2sns,
    aws2kinesis,
    fs2BlobstoreS3,
    fs2Kinesis,
    kinesisClient,
    protobuf,
    nettyCodec,
    sts,
    aws2sts
  )

  val gcpDependencies = Seq(
    fs2BlobstoreGCS,
    fs2PubSub,
    secretManager,
    gcpStorage
  )

  val azureDependencies = Seq(
    fs2BlobstoreAzure,
    azureIdentity,
    azureKeyVault,
    fs2Kafka,
    hadoopCommon,
    hadoopAzure,
    reactorNetty,
    jettyHttp
  )

  val commonDependencies = Seq(
    decline,
    analyticsSdk,
    badrows,
    igluClient,
    catsEffectKernel,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    circeLiteral,
    cron4sCirce,
    schemaDdl,
    http4sCore,
    aws2regions,
    jacksonDatabind,
    jacksonMapper,
    specs2,
    monocle,
    monocleMacro,
    catsRetry,
    fs2,
    ssm,
    log4cats,
    fs2BlobstoreCore,
    scalaTracker,
    scalaTrackerEmit,
    http4sClient,
    slf4jApi,
    sentry
  )

  val loaderDependencies = Seq(
    slf4jSimple,
    ssm,
    dynamodb,
    jSch,
    scalaTracker,
    scalaTrackerEmit,
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
    jettison,
    snappyJava,
    catsEffectLaws,
    catsTesting,
    catsEffectTestkit
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
    kinesis,
    dynamodb,
    cloudwatch,
    slf4jSimple,
    eventsManifest,
    sparkCore,
    sparkSQL,
    protobuf,
    commonsText,
    ivy,
    jacksonModule,
    jacksonDatabind,
    jacksonCbor,
    circeOptics,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  )

  val commonStreamTransformerDependencies = Seq(
    igluClientHttp4s,
    slf4jSimple,
    protobuf,
    log4cats,
    catsEffectLaws,
    circeOptics,
    parquet4s,
    hadoop,
    parquetHadoop,
    jsonSmart,
    nimbusJose,
    snappyJava,
    specs2,
    specs2ScalaCheck,
    scalaCheck,
    catsEffectTestkit,
    catsTesting,
    zookeeper
  )


  val transformerKinesisDependencies = Seq(
    dynamodb, // This dependency is required by hadoop-aws. It throws exception when it is not added.
    hadoopAws
  )

  val transformerPubsubDependencies = Seq(
    hadoopGcp
  )

  val transformerKafkaDependencies = Seq(
    hadoopAzure,
    jettison,
    eventGenerator
  )

  val commonExclusions =
    Seq(
      ExclusionRule(organization = "ch.qos.logback"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-api"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-yarn-client"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-mapreduce-client-jobclient"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-hdfs-client"),
      ExclusionRule(organization = "org.apache.hadoop.thirdparty", name = "hadoop-shaded-protobuf_3_7"),
    )
}
