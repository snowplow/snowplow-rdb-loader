/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._
import com.snowplowanalytics.snowplow.sbt.SnowplowSparkPlugin.autoImport.SparkDistribution

object Dependencies {

  object V {
    // Scala (Loader)
    val decline       = "2.4.1"
    val igluClient    = "4.2.1"
    val badrows       = "2.3.0"
    val analyticsSdk  = "3.1.0"
    val cron4sCirce   = "0.7.0"
    val circe         = "0.14.1"
    val circeConfig   = "0.10.0"
    val catsEffect    = "3.3.14"
    val fs2           = "3.12.1"
    val fs2Aws        = "4.1.0"
    val fs2Blobstore  = "0.9.14"
    val fs2Cron       = "0.9.0"
    val fs2PubSub     = "0.21.0"
    val fs2Kafka      = "3.0.0"
    val secretManager = "2.7.0"
    val gcpStorage    = "2.16.0"
    val azureIdentity = "1.18.4"
    val azureKeyVault = "4.6.2"
    val doobie        = "1.0.0-RC2"
    val monocle       = "2.0.3"
    val catsRetry     = "3.1.0"
    val log4cats      = "2.5.0"
    val http4s        = "0.23.18"
    val http4sBlaze   = "0.23.16" // this dep fell out of sync with http4s-core versioning - 0.23.16 is the last 0.X release.
    val scalaTracker  = "2.0.0"

    val spark              = "3.5.6"
    val sparkDocker        = "4.1.2" // Spark-on-Kubernetes Docker image — Scala 2.13 only
    val eventsManifest     = "0.5.0-M1"
    val schemaDdl          = "0.28.0"
    val jacksonModule      = "2.21.5" // Fix CVE - override incompatible version in spark runtime
    val parquet4s          = "2.22.0"
    val hadoopClient       = "3.5.0"
    val hadoopClientDocker = "3.4.3" // Matches the hadoop-client SnowplowSparkPlugin ships in the Spark 4.1.2 distribution
    val hadoopGcpClient    = "hadoop3-2.2.5"
    val parquetHadoop      = "1.18.0"

    val slf4j          = "2.0.17"
    val redshiftJdbc   = "2.2.2"
    val snowflakeJdbc  = "4.0.2"
    val databricksJdbc = "3.4.2"
    val enumeratum     = "1.7.0"
    val aws2           = "2.42.33"
    val pubsub         = "1.151.0"
    val jSch           = "0.2.15"
    val sentry         = "1.7.30"
    val kinesisClient  = "2.7.2"
    val refined =
      "0.11.3" // Override - fs2-aws-kinesis:4.1.0 pulls in 0.9.28 which depends on scala-xml:1.3.0, incompatible with Scala 2.12.21
    val nettyCodec    = "4.1.137.Final" // Fix CVE
    val zookeeper     = "3.9.5" // Fix CVE
    val kafkaClients  = "3.9.2" // Fix CVE
    val bouncyCastle  = "1.85" // Fix CVE
    val commonsConfig = "2.15.1" // Fix CVE
    val kerbyCore     = "2.1.2" // Fix CVE
    val lz4Java       = "1.11.1" // Fix CVE - kafka-clients ships 1.10.1
    val blazeHttp     = "0.23.18" // Fix CVE - http4s-blaze-client ships 0.23.16
    val nettySparkDistribution =
      "4.2.17.Final" // Fix CVE - SnowplowSparkPlugin pins 4.2.15.Final; Spark 4.1.2's Netty is on the 4.2.x line, distinct from our own nettyCodec (4.1.x)
    val jacksonSparkDistribution = "2.21.5" // Fix CVE - SnowplowSparkPlugin pins 2.21.4, still vulnerable
    val parquetJacksonSparkDistribution =
      "1.18.0" // Fix CVE - plugin pins 1.17.1, which shades jackson 2.21.3; deliberately ahead of the distribution's parquet 1.16.0

    // Scala (test only)
    val specs2            = "4.17.0"
    val catsTesting       = "1.5.0"
    val catsEffectTestkit = "3.4.5"
    val scalaCheck        = "1.17.0"
    val eventGenerator    = "0.4.0"

    val betterMonadicFor = "0.3.1"
  }

  val resolutionRepos = Seq(
    // Redshift native driver
    "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release/",
    ("Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true)
  )

  // Scala (Common)
  val http4sCore       = "org.http4s"    %% "http4s-core"        % V.http4s
  val http4sCirce      = "org.http4s"    %% "http4s-circe"       % V.http4s
  val catsEffectKernel = "org.typelevel" %% "cats-effect-kernel" % V.catsEffect

  // Scala (Loader)
  val decline           = "com.monovore"                  %% "decline"                  % V.decline
  val igluClient        = "com.snowplowanalytics"         %% "iglu-scala-client"        % V.igluClient
  val igluClientHttp4s  = "com.snowplowanalytics"         %% "iglu-scala-client-http4s" % V.igluClient
  val badrows           = "com.snowplowanalytics"         %% "snowplow-badrows"         % V.badrows
  val circeConfig       = "io.circe"                      %% "circe-config"             % V.circeConfig
  val circeGeneric      = "io.circe"                      %% "circe-generic"            % V.circe
  val circeGenericExtra = "io.circe"                      %% "circe-generic-extras"     % V.circe
  val cron4sCirce       = "com.github.alonsodomin.cron4s" %% "cron4s-circe"             % V.cron4sCirce
  val fs2               = "co.fs2"                        %% "fs2-core"                 % V.fs2
  val fs2Kafka = ("com.github.fd4s" %% "fs2-kafka" % V.fs2Kafka)
    .exclude("org.lz4", "lz4-java")
  val fs2Kinesis = ("io.laserdisc" %% "fs2-aws-kinesis" % V.fs2Aws)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.kinesis", "amazon-kinesis-client")
  val fs2BlobstoreS3    = "com.github.fs2-blobstore"   %% "s3"                                    % V.fs2Blobstore
  val fs2BlobstoreGCS   = "com.github.fs2-blobstore"   %% "gcs"                                   % V.fs2Blobstore
  val fs2BlobstoreAzure = "com.github.fs2-blobstore"   %% "azure"                                 % V.fs2Blobstore
  val azureIdentity     = "com.azure"                   % "azure-identity"                        % V.azureIdentity
  val azureKeyVault     = "com.azure"                   % "azure-security-keyvault-secrets"       % V.azureKeyVault
  val fs2Cron           = "eu.timepit"                 %% "fs2-cron-cron4s"                       % V.fs2Cron
  val fs2PubSub         = "com.permutive"              %% "fs2-google-pubsub-grpc"                % V.fs2PubSub
  val secretManager     = "com.google.cloud"            % "google-cloud-secretmanager"            % V.secretManager
  val gcpStorage        = "com.google.cloud"            % "google-cloud-storage"                  % V.gcpStorage
  val doobie            = "org.tpolecat"               %% "doobie-core"                           % V.doobie
  val doobieHikari      = "org.tpolecat"               %% "doobie-hikari"                         % V.doobie
  val analyticsSdk      = "com.snowplowanalytics"      %% "snowplow-scala-analytics-sdk"          % V.analyticsSdk
  val monocle           = "com.github.julien-truffaut" %% "monocle-core"                          % V.monocle
  val monocleMacro      = "com.github.julien-truffaut" %% "monocle-macro"                         % V.monocle
  val catsRetry         = "com.github.cb372"           %% "cats-retry"                            % V.catsRetry
  val log4cats          = "org.typelevel"              %% "log4cats-slf4j"                        % V.log4cats
  val http4sClient      = "org.http4s"                 %% "http4s-blaze-client"                   % V.http4sBlaze
  val scalaTracker      = "com.snowplowanalytics"      %% "snowplow-scala-tracker-core"           % V.scalaTracker
  val scalaTrackerEmit  = "com.snowplowanalytics"      %% "snowplow-scala-tracker-emitter-http4s" % V.scalaTracker

  // Scala (Shredder)
  val eventsManifest = "com.snowplowanalytics" %% "snowplow-events-manifest" % V.eventsManifest
  val schemaDdl      = "com.snowplowanalytics" %% "schema-ddl"               % V.schemaDdl

  val circeLiteral = "io.circe"         %% "circe-literal" % V.circe
  val circeOptics  = "io.circe"         %% "circe-optics"  % V.circe % Test
  val sparkCore    = "org.apache.spark" %% "spark-core"    % V.spark % Provided
  val sparkSQL     = "org.apache.spark" %% "spark-sql"     % V.spark % Provided
  val fs2Io        = "co.fs2"           %% "fs2-io"        % V.fs2

  val jacksonModule   = "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % V.jacksonModule
  val jacksonCbor     = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % V.jacksonModule
  val jacksonDatabind = "com.fasterxml.jackson.core"       % "jackson-databind"        % V.jacksonModule
  val parquet4s       = "com.github.mjakubowski84"        %% "parquet4s-fs2"           % V.parquet4s
  val hadoopCommon = ("org.apache.hadoop" % "hadoop-common" % V.hadoopClient)
    .exclude("com.jcraft", "jsch")
    .exclude("org.apache.zookeeper", "zookeeper")
    .exclude("org.eclipse.jetty", "jetty-server") // only used by Hadoop daemon web UIs, not client-side code
  val hadoop        = "org.apache.hadoop"  % "hadoop-client"  % V.hadoopClient
  val parquetHadoop = "org.apache.parquet" % "parquet-hadoop" % V.parquetHadoop
  val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopClient % Runtime)
    .excludeAll(ExclusionRule(organization = "software.amazon.awssdk")) // SDK v2 modules are already present in assembled jar
  val hadoopGcp   = "com.google.cloud.bigdataoss" % "gcs-connector" % V.hadoopGcpClient % Runtime
  val hadoopAzure = "org.apache.hadoop"           % "hadoop-azure"  % V.hadoopClient
  // hadoop-aws for the batch transformer Docker (Spark-on-Kubernetes) fat jar.
  // The EMR assembly excludes this jar via assemblyExcludedJars; EMR provides its own hadoop-aws.
  val hadoopAwsForDocker = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopClientDocker)
    .excludeAll(ExclusionRule(organization = "software.amazon.awssdk")) // individual SDK v2 modules already present in fat jar
  // Spark<->S3A committer bindings for the magic committer; not shipped in SnowplowSparkPlugin's
  // Spark distribution. intransitive() as the classes only need hadoop/parquet/spark already at runtime.
  val sparkHadoopCloud = ("org.apache.spark" %% "spark-hadoop-cloud" % V.sparkDocker % Runtime).intransitive()
  val kinesisClient = ("software.amazon.kinesis" % "amazon-kinesis-client" % V.kinesisClient)
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  // Java (Loader)
  val slf4jApi       = "org.slf4j"           % "slf4j-api"       % V.slf4j
  val slf4jSimple    = "org.slf4j"           % "slf4j-simple"    % V.slf4j
  val redshift       = "com.amazon.redshift" % "redshift-jdbc42" % V.redshiftJdbc
  val jSch           = "com.github.mwiede"   % "jsch"            % V.jSch
  val sentry         = "io.sentry"           % "sentry"          % V.sentry
  val snowflakeJdbc  = "net.snowflake"       % "snowflake-jdbc"  % V.snowflakeJdbc
  val databricksJdbc = "com.databricks"      % "databricks-jdbc" % V.databricksJdbc
  val enumeratum     = "com.beachape"       %% "enumeratum"      % V.enumeratum
  val kafkaClients = ("org.apache.kafka" % "kafka-clients" % V.kafkaClients)
    .exclude("org.lz4", "lz4-java")

  val aws2s3          = "software.amazon.awssdk" % "s3"                  % V.aws2
  val aws2cw          = "software.amazon.awssdk" % "cloudwatch"          % V.aws2
  val aws2dynamo      = "software.amazon.awssdk" % "dynamodb"            % V.aws2
  val aws2sqs         = "software.amazon.awssdk" % "sqs"                 % V.aws2
  val aws2sns         = "software.amazon.awssdk" % "sns"                 % V.aws2
  val aws2kinesis     = "software.amazon.awssdk" % "kinesis"             % V.aws2
  val aws2regions     = "software.amazon.awssdk" % "regions"             % V.aws2
  val aws2sts         = "software.amazon.awssdk" % "sts"                 % V.aws2 % Runtime
  val aws2ssm         = "software.amazon.awssdk" % "ssm"                 % V.aws2
  val aws2redshift    = "software.amazon.awssdk" % "redshift"            % V.aws2
  val awsS3Transfer   = "software.amazon.awssdk" % "s3-transfer-manager" % V.aws2 % Runtime
  val pubsub          = "com.google.cloud"       % "google-cloud-pubsub" % V.pubsub
  val refined         = "eu.timepit"            %% "refined"             % V.refined
  val nettyCodecHttp  = "io.netty"               % "netty-codec-http"    % V.nettyCodec
  val nettyCodecHttp2 = "io.netty"               % "netty-codec-http2"   % V.nettyCodec
  val nettyCodecSmtp  = "io.netty"               % "netty-codec-smtp"    % V.nettyCodec
  val nettyCodec      = "io.netty"               % "netty-codec"         % V.nettyCodec
  val nettyCodecDns   = "io.netty"               % "netty-codec-dns"     % V.nettyCodec
  val nettyAll        = "io.netty"               % "netty-all"           % V.nettyCodec
  val lz4Java         = "at.yawk.lz4"            % "lz4-java"            % V.lz4Java
  val blazeHttp       = "org.http4s"            %% "blaze-http"          % V.blazeHttp
  val zookeeper       = "org.apache.zookeeper"   % "zookeeper"           % V.zookeeper
  val bcprovJdk18on   = "org.bouncycastle"       % "bcprov-jdk18on"      % V.bouncyCastle

  val commonsConfiguration2 = "org.apache.commons" % "commons-configuration2" % V.commonsConfig
  val kerbyAsn1             = "org.apache.kerby"   % "kerby-asn1"             % V.kerbyCore

  // Scala (test only)
  val specs2            = "org.specs2"               %% "specs2-core"                   % V.specs2            % Test
  val specs2ScalaCheck  = "org.specs2"               %% "specs2-scalacheck"             % V.specs2            % Test
  val scalaCheck        = "org.scalacheck"           %% "scalacheck"                    % V.scalaCheck        % Test
  val catsTesting       = "org.typelevel"            %% "cats-effect-testing-specs2"    % V.catsTesting       % Test
  val catsEffectTestkit = "org.typelevel"            %% "cats-effect-testkit"           % V.catsEffectTestkit % Test
  val catsEffectLaws    = "org.typelevel"            %% "cats-effect-laws"              % V.catsEffect        % Test
  val fs2BlobstoreCore  = "com.github.fs2-blobstore" %% "core"                          % V.fs2Blobstore      % Test
  val eventGenerator    = "com.snowplowanalytics"    %% "snowplow-event-generator-core" % V.eventGenerator    % Test

  // compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  val awsDependencies = Seq(
    aws2s3,
    aws2cw,
    aws2dynamo,
    aws2sqs,
    aws2sns,
    aws2kinesis,
    aws2ssm,
    aws2redshift,
    fs2BlobstoreS3,
    fs2Kinesis,
    kinesisClient,
    aws2sts,
    refined, // Fix scala-xml conflict - fs2-aws-kinesis:4.1.0 pulls in refined:0.9.28 which depends on scala-xml:1.3.0, incompatible with Scala 2.12.21
    catsEffectTestkit
  )

  val gcpDependencies = Seq(
    fs2BlobstoreGCS,
    fs2PubSub,
    pubsub,
    secretManager,
    gcpStorage
  )

  val azureDependencies = Seq(
    fs2BlobstoreAzure,
    azureIdentity,
    azureKeyVault,
    fs2Kafka,
    kafkaClients,
    hadoopCommon,
    hadoopAzure,
    lz4Java // Fix CVE - kafka-clients ships the at.yawk.lz4 fork, which the org.lz4 exclusions do not cover
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
    specs2,
    monocle,
    monocleMacro,
    catsRetry,
    fs2,
    log4cats,
    fs2BlobstoreCore,
    scalaTracker,
    scalaTrackerEmit,
    http4sClient,
    slf4jApi,
    sentry,
    zookeeper,
    jacksonDatabind, // Fix CVE - multiple paths pull 2.21.2/2.21.3; force 2.21.5
    kerbyAsn1, // Fix CVE - hadoop-common ships 2.0.3
    blazeHttp // Fix CVE - blaze-http is released separately from http4s-blaze-client, which is now frozen
  )

  val loaderDependencies = Seq(
    slf4jSimple,
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
    fs2Io,
    catsEffectLaws,
    catsTesting,
    catsEffectTestkit,
    bcprovJdk18on, // Fix CVE - hadoop-common ships 1.82
    commonsConfiguration2, // Fix CVE - hadoop-common ships 2.10.1 (CVE-2026-45205)
    // Fix CVE - the loaders reach netty through azure-core-http-netty, which lags netty releases.
    // The transformers pin the same artifacts via nettyAll instead.
    nettyCodec,
    nettyCodecHttp,
    nettyCodecHttp2,
    nettyCodecDns
  )

  val redshiftDependencies = Seq(
    redshift
  )

  val snowflakeDependencies = Seq(
    enumeratum,
    snowflakeJdbc
  )

  val databricksDependencies = Seq(
    databricksJdbc
  )

  val batchTransformerDependencies = Seq(
    aws2s3,
    aws2sqs,
    aws2sns,
    aws2kinesis,
    aws2dynamo,
    aws2cw,
    aws2regions,
    slf4jSimple,
    eventsManifest,
    sparkCore,
    sparkSQL,
    jacksonModule,
    jacksonCbor,
    circeOptics,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  )

  // Extra dependencies for the batch transformer Docker (Spark-on-Kubernetes) distribution only.
  // Not needed for EMR: the cluster provides its own hadoop-aws, and IRSA is a K8s concept.
  val dockerOnlyDependencies = Seq(
    hadoopAwsForDocker,
    sparkHadoopCloud,
    aws2sts,
    awsS3Transfer
  )

  // Fix CVE - overrides for SnowplowSparkPlugin's own CVE pins on the Spark distribution
  // it bundles into /opt/spark/jars/ (the SparkDistribution config; see the plugin's
  // private `V` object). Coursier's highest-wins pulls these up over the plugin's
  // defaults without touching this app's own Compile classpath.
  val sparkDistributionOverrides = Seq(
    "io.netty" % "netty-buffer"                       % V.nettySparkDistribution,
    "io.netty" % "netty-codec"                        % V.nettySparkDistribution,
    "io.netty" % "netty-codec-base"                   % V.nettySparkDistribution,
    "io.netty" % "netty-codec-classes-quic"           % V.nettySparkDistribution,
    "io.netty" % "netty-codec-compression"            % V.nettySparkDistribution,
    "io.netty" % "netty-codec-dns"                    % V.nettySparkDistribution,
    "io.netty" % "netty-codec-http"                   % V.nettySparkDistribution,
    "io.netty" % "netty-codec-http2"                  % V.nettySparkDistribution,
    "io.netty" % "netty-codec-http3"                  % V.nettySparkDistribution,
    "io.netty" % "netty-codec-marshalling"            % V.nettySparkDistribution,
    "io.netty" % "netty-codec-protobuf"               % V.nettySparkDistribution,
    "io.netty" % "netty-codec-socks"                  % V.nettySparkDistribution,
    "io.netty" % "netty-common"                       % V.nettySparkDistribution,
    "io.netty" % "netty-handler"                      % V.nettySparkDistribution,
    "io.netty" % "netty-handler-proxy"                % V.nettySparkDistribution,
    "io.netty" % "netty-resolver"                     % V.nettySparkDistribution,
    "io.netty" % "netty-resolver-dns"                 % V.nettySparkDistribution,
    "io.netty" % "netty-transport"                    % V.nettySparkDistribution,
    "io.netty" % "netty-transport-classes-epoll"      % V.nettySparkDistribution,
    "io.netty" % "netty-transport-classes-io_uring"   % V.nettySparkDistribution,
    "io.netty" % "netty-transport-classes-kqueue"     % V.nettySparkDistribution,
    "io.netty" % "netty-transport-native-unix-common" % V.nettySparkDistribution,
    "io.netty" % "netty-codec-native-quic"            % V.nettySparkDistribution,
    "io.netty" % "netty-transport-native-epoll"       % V.nettySparkDistribution,
    "io.netty" % "netty-transport-native-io_uring"    % V.nettySparkDistribution,
    "io.netty" % "netty-transport-native-kqueue"      % V.nettySparkDistribution,
    "at.yawk.lz4" % "lz4-java" % V.lz4Java, // Fix CVE - the plugin only overrides org.lz4:lz4-java, not this fork (kafka-clients ships the at.yawk.lz4 fork, same as our own lz4Java override elsewhere)
    "org.apache.parquet"         % "parquet-jackson"  % V.parquetJacksonSparkDistribution, // Fix CVE - shades jackson-databind/jackson-core
    "com.fasterxml.jackson.core" % "jackson-databind" % V.jacksonSparkDistribution,
    "com.fasterxml.jackson.core" % "jackson-core"     % V.jacksonSparkDistribution,
    "com.fasterxml.jackson.datatype"   % "jackson-datatype-jsr310" % V.jacksonSparkDistribution,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % V.jacksonSparkDistribution,
    "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % V.jacksonSparkDistribution,
    "org.bouncycastle"                 % "bcprov-jdk18on"          % V.bouncyCastle // Fix CVE - zookeeper/hadoop pull 1.80
  ).map(_ % SparkDistribution)

  // Like batchTransformerDependencies but compiled against Spark 4.x / Scala 2.13.
  // Spark itself is declared Provided in BuildSettings (see sparkVersion), not here,
  // so it is not bundled into the app fat jar — SnowplowSparkPlugin's distribution
  // supplies it at runtime.
  val batchTransformerDockerDependencies = Seq(
    aws2s3,
    aws2sqs,
    aws2sns,
    aws2kinesis,
    aws2dynamo,
    aws2cw,
    aws2regions,
    slf4jSimple,
    eventsManifest,
    jacksonModule,
    jacksonCbor,
    circeOptics,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  )

  val commonStreamTransformerDependencies = Seq(
    igluClientHttp4s,
    slf4jApi,
    slf4jSimple,
    log4cats,
    catsEffectLaws,
    circeOptics,
    parquet4s,
    hadoop,
    parquetHadoop,
    fs2Io,
    nettyCodecHttp,
    nettyCodecHttp2,
    nettyAll, // Fix CVE - hadoop ships netty-all:4.1.130.Final; also pins codec-smtp, handler-proxy, codec-dns, codec-redis, codec-mqtt
    bcprovJdk18on, // Fix CVE - hadoop-common ships 1.82
    commonsConfiguration2, // Fix CVE - hadoop-common ships 2.10.1 (CVE-2026-45205)
    specs2,
    specs2ScalaCheck,
    scalaCheck,
    catsEffectTestkit,
    catsTesting
  )

  val transformerKinesisDependencies = Seq(
    hadoopAws
  )

  val transformerPubsubDependencies = Seq(
    hadoopGcp
  )

  val transformerKafkaDependencies = Seq(
    hadoopAzure,
    eventGenerator
  )

  val commonExclusions =
    Seq(
      ExclusionRule(organization = "ch.qos.logback"),
      // Hadoop's servlet stack, only used by its daemon web UIs and not by client-side code.
      // Jetty 9.4 is EOL, so CVEs in these artifacts will never be fixed at the version Hadoop asks for.
      ExclusionRule(organization = "org.eclipse.jetty", name            = "jetty-servlet"),
      ExclusionRule(organization = "org.eclipse.jetty", name            = "jetty-webapp"),
      ExclusionRule(organization = "org.eclipse.jetty", name            = "jetty-security"),
      ExclusionRule(organization = "org.apache.hadoop", name            = "hadoop-yarn-api"),
      ExclusionRule(organization = "org.apache.hadoop", name            = "hadoop-yarn-client"),
      ExclusionRule(organization = "org.apache.hadoop", name            = "hadoop-mapreduce-client-jobclient"),
      ExclusionRule(organization = "org.apache.hadoop", name            = "hadoop-hdfs-client"),
      ExclusionRule(organization = "org.apache.hadoop.thirdparty", name = "hadoop-shaded-protobuf_3_7"),
      ExclusionRule(organization = "org.apache.hadoop.thirdparty", name = "hadoop-shaded-protobuf_3_21")
    )
}
