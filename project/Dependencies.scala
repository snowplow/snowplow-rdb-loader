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

object Dependencies {

  object V {
    // Scala and FP core
    val decline     = "2.4.1"
    val circe       = "0.14.1"
    val circeConfig = "0.10.0"
    val cron4sCirce = "0.7.0"
    val catsEffect  = "3.3.14"
    val fs2         = "3.14.0"
    val fs2Cron     = "0.9.0"
    val doobie      = "1.0.0-RC2"
    val monocle     = "2.0.3"
    val catsRetry   = "3.1.0"
    val log4cats    = "2.5.0"
    val http4s      = "0.23.37"
    val http4sBlaze = "0.23.18" // versions independently of http4s-core, and trails it

    // Snowplow libraries
    val igluClient     = "4.2.1"
    val badrows        = "2.3.0"
    val analyticsSdk   = "3.2.3"
    val schemaDdl      = "0.28.0"
    val scalaTracker   = "2.0.0"
    val eventsManifest = "0.5.0-M1"

    // Cloud SDKs and blob storage
    val aws2          = "2.55.3"
    val gcpStorage    = "2.73.0"
    val secretManager = "2.7.0"
    val azureIdentity = "1.18.6"
    val azureKeyVault = "4.11.2"
    val fs2Blobstore  = "0.9.14"

    // Messaging and streaming
    val fs2Aws        = "4.1.0"
    val kinesisClient = "2.7.3"
    val fs2Kafka      = "3.0.0"
    val fs2PubSub     = "0.21.0"
    val pubsub        = "1.155.0"

    // Warehouse drivers, and the SSH tunnel the Redshift loader opens
    val redshiftJdbc   = "2.2.9"
    val snowflakeJdbc  = "4.4.0"
    val databricksJdbc = "3.4.3"
    val enumeratum     = "1.7.0"
    val jSch           = "2.28.7"

    // Spark, Hadoop and Parquet
    val spark              = "3.5.9"
    val sparkDocker        = "4.2.0" // Spark-on-Kubernetes Docker image — Scala 2.13 only
    val hadoopClient       = "3.5.0"
    val hadoopClientDocker = "3.5.0" // keep in step with the hadoop-client sparkDocker brings in
    val hadoopGcpClient    = "hadoop3-2.2.5"
    val parquet4s          = "2.22.0"

    // Logging and error reporting
    val slf4j  = "2.0.17"
    val sentry = "1.7.30"

    // Versions held above what something upstream asks for, to keep a vulnerable copy out of the
    // images. See the *SecurityOverrides Seqs below for which artifacts each one governs.
    val jacksonModule = "2.21.7"
    val parquetHadoop = "1.18.1"
    val nettyCodec    = "4.1.138.Final"
    val zookeeper     = "3.9.6"
    val kafkaClients  = "3.9.2"
    val bouncyCastle  = "1.86"
    val commonsConfig = "2.15.1"
    val kerbyCore     = "2.1.2"
    val lz4Java       = "1.11.4"
    val zstdJni       = "1.5.7-19"

    // Held above upstream for binary compatibility rather than security
    // Held above what fs2-aws-kinesis asks for: its version wants a scala-xml too old for our Scala 2.12
    val refined = "0.11.3"

    // Test only
    val specs2            = "4.17.0"
    val catsTesting       = "1.5.0"
    val catsEffectTestkit = "3.4.5"
    val scalaCheck        = "1.17.0"
    val eventGenerator    = "0.4.0"

    // Compiler plugins
    val betterMonadicFor = "0.3.1"
  }

  val resolutionRepos = Seq(
    // Redshift native driver
    "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release/",
    ("Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true)
  )
  // Scala and FP core
  val http4sCore        = "org.http4s"                    %% "http4s-core"          % V.http4s
  val http4sCirce       = "org.http4s"                    %% "http4s-circe"         % V.http4s
  val http4sClient      = "org.http4s"                    %% "http4s-blaze-client"  % V.http4sBlaze
  val catsEffectKernel  = "org.typelevel"                 %% "cats-effect-kernel"   % V.catsEffect
  val decline           = "com.monovore"                  %% "decline"              % V.decline
  val circeConfig       = "io.circe"                      %% "circe-config"         % V.circeConfig
  val circeGeneric      = "io.circe"                      %% "circe-generic"        % V.circe
  val circeGenericExtra = "io.circe"                      %% "circe-generic-extras" % V.circe
  val circeLiteral      = "io.circe"                      %% "circe-literal"        % V.circe
  val cron4sCirce       = "com.github.alonsodomin.cron4s" %% "cron4s-circe"         % V.cron4sCirce
  val fs2               = "co.fs2"                        %% "fs2-core"             % V.fs2
  val fs2Io             = "co.fs2"                        %% "fs2-io"               % V.fs2
  val fs2Cron           = "eu.timepit"                    %% "fs2-cron-cron4s"      % V.fs2Cron
  val doobie            = "org.tpolecat"                  %% "doobie-core"          % V.doobie
  val doobieHikari      = "org.tpolecat"                  %% "doobie-hikari"        % V.doobie
  val monocle           = "com.github.julien-truffaut"    %% "monocle-core"         % V.monocle
  val monocleMacro      = "com.github.julien-truffaut"    %% "monocle-macro"        % V.monocle
  val catsRetry         = "com.github.cb372"              %% "cats-retry"           % V.catsRetry
  val log4cats          = "org.typelevel"                 %% "log4cats-slf4j"       % V.log4cats

  // Snowplow libraries
  val igluClient       = "com.snowplowanalytics" %% "iglu-scala-client"                     % V.igluClient
  val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s"              % V.igluClient
  val badrows          = "com.snowplowanalytics" %% "snowplow-badrows"                      % V.badrows
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk"          % V.analyticsSdk
  val schemaDdl        = "com.snowplowanalytics" %% "schema-ddl"                            % V.schemaDdl
  val scalaTracker     = "com.snowplowanalytics" %% "snowplow-scala-tracker-core"           % V.scalaTracker
  val scalaTrackerEmit = "com.snowplowanalytics" %% "snowplow-scala-tracker-emitter-http4s" % V.scalaTracker
  val eventsManifest   = "com.snowplowanalytics" %% "snowplow-events-manifest"              % V.eventsManifest

  // Cloud SDKs and blob storage
  val aws2s3        = "software.amazon.awssdk" % "s3"                  % V.aws2
  val aws2cw        = "software.amazon.awssdk" % "cloudwatch"          % V.aws2
  val aws2dynamo    = "software.amazon.awssdk" % "dynamodb"            % V.aws2
  val aws2sqs       = "software.amazon.awssdk" % "sqs"                 % V.aws2
  val aws2sns       = "software.amazon.awssdk" % "sns"                 % V.aws2
  val aws2kinesis   = "software.amazon.awssdk" % "kinesis"             % V.aws2
  val aws2regions   = "software.amazon.awssdk" % "regions"             % V.aws2
  val aws2sts       = "software.amazon.awssdk" % "sts"                 % V.aws2 % Runtime
  val aws2ssm       = "software.amazon.awssdk" % "ssm"                 % V.aws2
  val aws2redshift  = "software.amazon.awssdk" % "redshift"            % V.aws2
  val awsS3Transfer = "software.amazon.awssdk" % "s3-transfer-manager" % V.aws2 % Runtime
  // hadoop-aws's S3A builds its client with ApacheHttpClient by name, but the SDK now pulls in apache5-client instead
  val awsApacheClient   = "software.amazon.awssdk"    % "apache-client"                   % V.aws2 % Runtime
  val gcpStorage        = "com.google.cloud"          % "google-cloud-storage"            % V.gcpStorage
  val secretManager     = "com.google.cloud"          % "google-cloud-secretmanager"      % V.secretManager
  val azureIdentity     = "com.azure"                 % "azure-identity"                  % V.azureIdentity
  val azureKeyVault     = "com.azure"                 % "azure-security-keyvault-secrets" % V.azureKeyVault
  val fs2BlobstoreS3    = "com.github.fs2-blobstore" %% "s3"                              % V.fs2Blobstore
  val fs2BlobstoreGCS   = "com.github.fs2-blobstore" %% "gcs"                             % V.fs2Blobstore
  val fs2BlobstoreAzure = "com.github.fs2-blobstore" %% "azure"                           % V.fs2Blobstore

  // Messaging and streaming
  val fs2Kinesis = ("io.laserdisc" %% "fs2-aws-kinesis" % V.fs2Aws)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.kinesis", "amazon-kinesis-client")
  val kinesisClient = ("software.amazon.kinesis" % "amazon-kinesis-client" % V.kinesisClient)
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  val fs2Kafka = ("com.github.fd4s" %% "fs2-kafka" % V.fs2Kafka)
    .exclude("org.lz4", "lz4-java")
  val kafkaClients = ("org.apache.kafka" % "kafka-clients" % V.kafkaClients)
    .exclude("org.lz4", "lz4-java")
  val fs2PubSub = "com.permutive"   %% "fs2-google-pubsub-grpc" % V.fs2PubSub
  val pubsub    = "com.google.cloud" % "google-cloud-pubsub"    % V.pubsub

  // Warehouse drivers, and the SSH tunnel the Redshift loader opens
  val redshift       = "com.amazon.redshift" % "redshift-jdbc42"     % V.redshiftJdbc
  val snowflakeJdbc  = "net.snowflake"       % "snowflake-jdbc-thin" % V.snowflakeJdbc
  val databricksJdbc = "com.databricks"      % "databricks-jdbc"     % V.databricksJdbc
  val enumeratum     = "com.beachape"       %% "enumeratum"          % V.enumeratum
  val jSch           = "com.github.mwiede"   % "jsch"                % V.jSch

  // Spark, Hadoop and Parquet
  val sparkCore = "org.apache.spark" %% "spark-core" % V.spark % Provided
  val sparkSQL  = "org.apache.spark" %% "spark-sql"  % V.spark % Provided
  // Spark<->S3A committer bindings for the magic committer; not shipped in SnowplowSparkPlugin's
  // Spark distribution. intransitive() as the classes only need hadoop/parquet/spark already at runtime.
  val sparkHadoopCloud = ("org.apache.spark" %% "spark-hadoop-cloud" % V.sparkDocker % Runtime).intransitive()
  val hadoop           = "org.apache.hadoop"  % "hadoop-client"      % V.hadoopClient
  val hadoopCommon = ("org.apache.hadoop" % "hadoop-common" % V.hadoopClient)
    .exclude("com.jcraft", "jsch")
    .exclude("org.apache.zookeeper", "zookeeper")
    .exclude("org.eclipse.jetty", "jetty-server") // only used by Hadoop daemon web UIs, not client-side code
  val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopClient % Runtime)
    .excludeAll(ExclusionRule(organization = "software.amazon.awssdk")) // SDK v2 modules are already present in assembled jar
  // hadoop-aws for the batch transformer Docker (Spark-on-Kubernetes) fat jar.
  // The EMR assembly excludes this jar via assemblyExcludedJars; EMR provides its own hadoop-aws.
  val hadoopAwsForDocker = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopClientDocker)
    .excludeAll(ExclusionRule(organization = "software.amazon.awssdk")) // individual SDK v2 modules already present in fat jar
  val hadoopGcp   = "com.google.cloud.bigdataoss" % "gcs-connector" % V.hadoopGcpClient % Runtime
  val hadoopAzure = "org.apache.hadoop"           % "hadoop-azure"  % V.hadoopClient
  val parquet4s   = "com.github.mjakubowski84"   %% "parquet4s-fs2" % V.parquet4s

  // Logging and error reporting
  val slf4jApi    = "org.slf4j" % "slf4j-api"    % V.slf4j
  val slf4jSimple = "org.slf4j" % "slf4j-simple" % V.slf4j
  val sentry      = "io.sentry" % "sentry"       % V.sentry

  // Artifacts named only to force a version up; grouped into Seqs by SecurityOverrides below
  val nettyCodecDns         = "io.netty"                         % "netty-codec-dns"         % V.nettyCodec
  val nettyAll              = "io.netty"                         % "netty-all"               % V.nettyCodec
  val lz4Java               = "at.yawk.lz4"                      % "lz4-java"                % V.lz4Java
  val zstdJni               = "com.github.luben"                 % "zstd-jni"                % V.zstdJni
  val zookeeper             = "org.apache.zookeeper"             % "zookeeper"               % V.zookeeper
  val bcprovJdk18on         = "org.bouncycastle"                 % "bcprov-jdk18on"          % V.bouncyCastle
  val commonsConfiguration2 = "org.apache.commons"               % "commons-configuration2"  % V.commonsConfig
  val kerbyAsn1             = "org.apache.kerby"                 % "kerby-asn1"              % V.kerbyCore
  val jacksonDatabind       = "com.fasterxml.jackson.core"       % "jackson-databind"        % V.jacksonModule
  val jacksonModule         = "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % V.jacksonModule
  val jacksonCbor           = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % V.jacksonModule
  val parquetHadoop         = "org.apache.parquet"               % "parquet-hadoop"          % V.parquetHadoop

  // Named to force a version up for binary compatibility rather than security
  val refined = "eu.timepit" %% "refined" % V.refined

  // Test only
  val specs2            = "org.specs2"               %% "specs2-core"                   % V.specs2            % Test
  val specs2ScalaCheck  = "org.specs2"               %% "specs2-scalacheck"             % V.specs2            % Test
  val scalaCheck        = "org.scalacheck"           %% "scalacheck"                    % V.scalaCheck        % Test
  val catsTesting       = "org.typelevel"            %% "cats-effect-testing-specs2"    % V.catsTesting       % Test
  val catsEffectTestkit = "org.typelevel"            %% "cats-effect-testkit"           % V.catsEffectTestkit % Test
  val catsEffectLaws    = "org.typelevel"            %% "cats-effect-laws"              % V.catsEffect        % Test
  val fs2BlobstoreCore  = "com.github.fs2-blobstore" %% "core"                          % V.fs2Blobstore      % Test
  val circeOptics       = "io.circe"                 %% "circe-optics"                  % V.circe             % Test
  val eventGenerator    = "com.snowplowanalytics"    %% "snowplow-event-generator-core" % V.eventGenerator    % Test

  // Compiler plugins
  val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

  // Security overrides, grouped by the module that drags the vulnerable copy in. An entry earns its
  // place only while it still evicts something: once the upstream catches up, delete it. To check,
  // drop the line and compare the resolved runtime classpath before and after — reading the
  // dependency tree for eviction markers is unreliable, because sbt truncates long lines.
  val commonSecurityOverrides = Seq(jacksonDatabind)

  // Kafka's client pulls compression codecs that lag their own fixes, and ships the at.yawk lz4
  // fork, which the org.lz4 exclusions elsewhere do not cover.
  val azureSecurityOverrides = Seq(lz4Java, zstdJni)

  // The loaders reach netty-codec-dns through the Azure SDK's HTTP client, which trails Netty's
  // releases. BouncyCastle, commons-configuration2 and kerby come in via Hadoop.
  val loaderSecurityOverrides =
    Seq(bcprovJdk18on, commonsConfiguration2, kerbyAsn1, nettyCodecDns)

  // The stream transformers reach the same libraries through Hadoop instead, which also brings in
  // ZooKeeper and netty-all. parquet-hadoop arrives via parquet4s.
  val streamTransformerSecurityOverrides =
    Seq(nettyAll, bcprovJdk18on, commonsConfiguration2, kerbyAsn1, zookeeper, parquetHadoop, zstdJni)

  // Spark and the AWS SDK are what reach for Jackson at runtime on EMR. The Docker distribution
  // gets its Jackson from the Spark distribution instead.
  val batchTransformerSecurityOverrides = Seq(jacksonModule, jacksonCbor)

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
    hadoopAzure
  ) ++ azureSecurityOverrides

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
    sentry
  ) ++ commonSecurityOverrides

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
    catsEffectTestkit
  ) ++ loaderSecurityOverrides

  val redshiftDependencies = Seq(
    redshift
  )

  val snowflakeDependencies = Seq(
    enumeratum,
    snowflakeJdbc,
    awsS3Transfer
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
    circeOptics,
    specs2,
    specs2ScalaCheck,
    scalaCheck
  ) ++ batchTransformerSecurityOverrides

  // Extra dependencies for the batch transformer Docker (Spark-on-Kubernetes) distribution only.
  // Not needed for EMR: the cluster provides its own hadoop-aws, IRSA is a K8s concept, and EMR's
  // own SDK supplies the Apache HTTP client that S3A asks for.
  val dockerOnlyDependencies = Seq(
    hadoopAwsForDocker,
    sparkHadoopCloud,
    aws2sts,
    awsS3Transfer,
    awsApacheClient
  )

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
    eventsManifest,
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
    fs2Io,
    specs2,
    specs2ScalaCheck,
    scalaCheck,
    catsEffectTestkit,
    catsTesting
  ) ++ streamTransformerSecurityOverrides

  val transformerKinesisDependencies = Seq(
    hadoopAws,
    awsS3Transfer,
    awsApacheClient
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
