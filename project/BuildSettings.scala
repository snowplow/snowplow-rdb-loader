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

// SBT
import sbt._
import Keys._
import sbtbuildinfo.BuildInfoPlugin.autoImport._

// sbt-assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

// sbt-native-packager
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

import com.snowplowanalytics.snowplow.sbt.IgluSchemaPlugin.autoImport._
import com.snowplowanalytics.snowplow.sbt.SnowplowSparkPlugin.autoImport._

import scoverage.ScoverageKeys._

import sbtdynver.DynVerPlugin.autoImport._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

import org.typelevel.sbt.tpolecat.TpolecatPlugin.autoImport._
import org.typelevel.scalacoptions.ScalacOptions._

/**
 * Common settings-patterns for Snowplow apps and libraries. To enable any of these you need to
 * explicitly add Settings value to build.sbt
 */
object BuildSettings {

  /**
   * Common set of useful and restrictive compiler flags
   */
  lazy val buildSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.21",
    Compile / unmanagedResources += file("SNOWPLOW-LICENSE.md"),
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.4" cross CrossVersion.full),
    tpolecatExcludeOptions ++= Set(fatalWarnings),
    resolvers ++= Dependencies.resolutionRepos
  ) ++ formattingSettings

  // sbt-assembly settings
  lazy val jarName = assembly / assemblyJarName := name.value + "-" + version.value + ".jar"

  lazy val formattingSettings = Seq(
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false
  )

  // Jars excluded from all transformer assemblies (EMR and Docker alike).
  // hadoop-aws is intentionally absent here — it is excluded from the EMR assembly only,
  // via an override in transformerBatchBuildSettings below.
  private val baseAssemblyExcludes: Set[String] = Set(
    "jsp-api-2.1-6.1.14.jar",
    "jsp-2.1-6.1.14.jar",
    "jasper-compiler-5.5.12.jar",
    "minlog-1.2.jar", // Otherwise causes conflicts with Kyro (which bundles it)
    "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
    "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
    "commons-beanutils-1.7.0.jar", // "
    "hadoop-core-1.1.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
    "hadoop-tools-1.1.2.jar" // "
  )

  lazy val transformerAssemblySettings = Seq(
    jarName,
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp.filter(jar => baseAssemblyExcludes(jar.data.getName))
    },
    assembly / assemblyMergeStrategy := {
      case "project.clj"                                                     => MergeStrategy.discard // Leiningen build files
      case PathList("META-INF", "services", _*)                              => MergeStrategy.concat // preserve ServiceLoader registrations
      case x if x.startsWith("META-INF")                                     => MergeStrategy.discard
      case x if x.endsWith(".html")                                          => MergeStrategy.discard
      case x if x.endsWith("package-info.class")                             => MergeStrategy.first
      case x if x.endsWith("customization.config")                           => MergeStrategy.first
      case x if x.endsWith("examples-1.json")                                => MergeStrategy.first
      case x if x.endsWith("paginators-1.json")                              => MergeStrategy.first
      case x if x.endsWith("service-2.json")                                 => MergeStrategy.first
      case x if x.endsWith("waiters-2.json")                                 => MergeStrategy.first
      case x if x.endsWith("mime.types")                                     => MergeStrategy.first
      case x if x.endsWith("module-info.class")                              => MergeStrategy.discard
      case PathList("com", "google", "common", _)                            => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", _)                   => MergeStrategy.first
      case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first // http4s, 2.13 shim
      case PathList("org", "apache", "commons", "logging", _ @_*)            => MergeStrategy.first
      case PathList("org", "slf4j", "impl", _)                               => MergeStrategy.first
      case "AUTHORS"                                                         => MergeStrategy.discard
      case PathList("com", "snowplowanalytics", "snowplow", "rdbloader", "generated", "ProjectMetadata.class" | "ProjectMetadata$.class") =>
        MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("cats.**" -> "shadecats.@1").inAll,
      ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
    )
  ) ++ (if (sys.env.get("SKIP_TEST").contains("true")) Seq(assembly / test := {}) else Seq())

  lazy val scoverageSettings = Seq(
    coverageMinimumStmtTotal := 50,
    coverageFailOnMinimum := false,
    (Test / test) := {
      (coverageReport dependsOn (Test / test)).value
    }
  )

  /** Add `config` directory as a resource */
  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )

  lazy val oneJvmPerTestSetting =
    Test / testGrouping := {
      val jvmOpts = (Test / javaOptions).value
      (Test / definedTests).value map { test =>
        val forkOptions = ForkOptions()
          .withJavaHome(javaHome.value)
          .withOutputStrategy(outputStrategy.value)
          .withBootJars(Vector.empty)
          .withWorkingDirectory(Option(baseDirectory.value))
          .withConnectInput(connectInput.value)
          .withRunJVMOptions(jvmOpts.toVector)

        val runPolicy = Tests.SubProcess(forkOptions)
        Tests.Group(name = test.name, tests = Seq(test), runPolicy = runPolicy)
      }
    }

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
  )

  lazy val additionalDockerSettings = Seq(
    Universal / mappings += file("SNOWPLOW-LICENSE.md") -> "/SNOWPLOW-LICENSE.md"
  )

  lazy val awsBuildSettings =
    buildSettings

  lazy val gcpBuildSettings =
    buildSettings

  lazy val azureBuildSettings =
    buildSettings

  lazy val commonBuildSettings =
    Seq(
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.generated"
    ) ++ scoverageSettings ++ buildSettings

  lazy val commonStreamTransformerBuildSettings =
    Seq(
      Test / igluUris := Seq(
        "iglu:com.google.analytics/cookies/jsonschema/1-0-0",
        "iglu:com.google.analytics/private/jsonschema/1-0-0",
        "iglu:org.ietf/http_cookie/jsonschema/1-0-0",
        "iglu:org.ietf/http_header/jsonschema/1-0-0",
        "iglu:com.mparticle.snowplow/pushregistration_event/jsonschema/1-0-0",
        "iglu:com.mparticle.snowplow/session_context/jsonschema/1-0-0",
        "iglu:com.optimizely.optimizelyx/summary/jsonschema/1-0-0",
        "iglu:com.optimizely/state/jsonschema/1-0-0",
        "iglu:com.optimizely/variation/jsonschema/1-0-0",
        "iglu:com.optimizely/visitor/jsonschema/1-0-0",
        "iglu:com.segment/screen/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1",
        "iglu:com.snowplowanalytics.snowplow/consent_document/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/desktop_context/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
        "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0",
        "iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0",
        "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0"
      )
    ) ++ buildSettings ++ addExampleConfToTestCp ++ dynVerSettings

  lazy val loaderBuildSettings =
    buildSettings ++ addExampleConfToTestCp ++ dynVerSettings

  lazy val redshiftBuildSettings =
    Seq(
      name := "snowplow-redshift-loader",
      Docker / packageName := "rdb-loader-redshift",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.redshift._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.redshift.Main")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ dynVerSettings

  // The Snowflake JDBC driver (4.x+) uses Apache Arrow internally, which requires access to
  // java.nio internals that Java's module system (JPMS) restricts by default.
  // (The Databricks OSS JDBC driver 3.3.1+ has its own on-heap Arrow path and doesn't need this.)
  private val arrowJvmOptions = List("--add-opens=java.base/java.nio=ALL-UNNAMED")

  // Routes Snowflake JDBC logging through SLF4J so it respects the application's log level.
  private val snowflakeJvmOptions = arrowJvmOptions :+ "-Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger"

  private lazy val snowflakeDockerSettings = Seq(
    dockerEnvVars += ("JAVA_OPTS" -> snowflakeJvmOptions.mkString(" "))
  )

  private lazy val snowflakeDistrolessDockerSettings = Seq(
    dockerEntrypoint := {
      val orig = dockerEntrypoint.value
      orig.head +: snowflakeJvmOptions ++: orig.tail
    }
  )

  lazy val snowflakeBuildSettings =
    Seq(
      name := "snowplow-snowflake-loader",
      Docker / packageName := "rdb-loader-snowflake",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.snowflake._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.snowflake.Main")
    ) ++ snowflakeDockerSettings ++ buildSettings ++ addExampleConfToTestCp ++ dynVerSettings

  lazy val snowflakeDistrolessBuildSettings =
    snowflakeBuildSettings.diff(snowflakeDockerSettings) ++ snowflakeDistrolessDockerSettings

  lazy val databricksBuildSettings =
    Seq(
      name := "snowplow-databricks-loader",
      Docker / packageName := "rdb-loader-databricks",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.databricks._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.databricks.Main"),
      // used in extended configuration parsing unit tests
      Test / envVars := Map(
        "OAUTH_CLIENT_SECRET" -> "client-secret"
      ),
      // envVars works only when fork is enabled
      Test / fork := true
    ) ++ buildSettings ++ addExampleConfToTestCp ++ dynVerSettings

  lazy val databricksDistrolessBuildSettings = databricksBuildSettings

  lazy val transformerBatchBuildSettings =
    Seq(
      name := "snowplow-transformer-batch",
      description := "Spark job to transform Snowplow enriched events into DB/query-friendly format",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated",
      buildInfoKeys := List(name, version, description),
      // Spark accesses JDK internals that Java 17 hides by default.
      // EMR 7.x injects these flags automatically; we need to replicate them for local test JVMs.
      Test / javaOptions ++= Seq(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
      ),
      assembly / assemblyShadeRules := Seq(
        ShadeRule
          .rename(
            // EMR has 0.1.42 installed
            "com.jcraft.jsch.**" -> "shadejsch.@1"
          )
          .inProject
      ),
      assembly / target := file("modules/transformer-batch/target/scala-2.12/assembled-jar"),
      BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
    ) ++ buildSettings ++ transformerAssemblySettings ++ dynVerSettings ++ addExampleConfToTestCp

  // The S3A magic committer, which avoids the default committer's slow per-file S3 renames
  // at job commit. The commit protocol / parquet committer classes come from spark-hadoop-cloud
  // (bundled via Dependencies.dockerOnlyDependencies). The factory is bound by URI scheme: we
  // write s3:// paths (fs.s3.impl is remapped to S3AFileSystem below), so scheme.s3 is the
  // binding that matters — scheme.s3a is included for s3a://-configured output.
  private val magicCommitterSparkConfig = Map(
    "spark.hadoop.fs.s3a.committer.name" -> "magic",
    "spark.hadoop.fs.s3a.committer.magic.enabled" -> "true",
    "spark.sql.sources.commitProtocolClass" -> "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
    "spark.sql.parquet.output.committer.class" -> "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3" -> "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" -> "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
  )

  // Spark configuration baked into the image's spark-defaults.conf (Spark's
  // lowest-precedence source, so deploy-time --conf still overrides it). These
  // wire S3 access to the S3A filesystem and our own credentials provider.
  private val batchTransformerSparkConfig = Map(
    "spark.hadoop.fs.s3.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.AbstractFileSystem.s3.impl" -> "org.apache.hadoop.fs.s3a.S3A",
    "spark.hadoop.fs.s3a.aws.credentials.provider" -> "com.snowplowanalytics.snowplow.rdbloader.transformer.batch.aws.S3ADefaultCredentialsProvider",
    "spark.hadoop.fs.s3.aws.credentials.provider" -> "com.snowplowanalytics.snowplow.rdbloader.transformer.batch.aws.S3ADefaultCredentialsProvider"
  ) ++ magicCommitterSparkConfig

  // The Spark-on-Kubernetes distribution of the batch transformer. The generic
  // Spark packaging — the self-deploying entrypoint, the vendored Spark launch
  // scripts, the /opt/spark/jars distribution closure and its CVE pins, the
  // eclipse-temurin base image and the driver/executor dispatch — all lives in
  // SnowplowSparkPlugin. Everything below is the application-specific half: e.g.
  // the Spark version we ship and compile against, and the intrinsic Spark conf.
  lazy val transformerBatchDockerBuildSettings =
    Seq(
      name := "snowplow-transformer-batch-docker",
      description := "Spark job to transform Snowplow enriched events into DB/query-friendly format (Spark-on-Kubernetes distribution)",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated",
      buildInfoKeys := List(name, version, description),
      Docker / packageName := "rdb-transformer-batch",
      // Pin the shipped Spark version (and, via sparkVersion.value below, the
      // version we compile against) rather than inheriting the plugin's default.
      sparkVersion := Dependencies.V.sparkDocker,
      sparkConfig := batchTransformerSparkConfig,
      // The app compiles against Spark but does not bundle it: the distribution
      // supplies Spark at runtime from /opt/spark/jars. spark-sql pulls spark-core
      // transitively, which is all the app's sources need to compile.
      libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided
    ) ++ buildSettings ++ Seq(
      // buildSettings sets scalaVersion := "2.12.21"; override here because Spark 4.x
      // only publishes Scala 2.13 artifacts.
      scalaVersion := "2.13.18"
    ) ++ transformerAssemblySettings ++ dynVerSettings ++ additionalDockerSettings

  lazy val transformerKinesisBuildSettings =
    Seq(
      name := "snowplow-transformer-kinesis",
      Docker / packageName := "transformer-kinesis",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated",
      buildInfoKeys := List(name, version, description)
    ) ++ buildSettings ++ dynVerSettings ++ addExampleConfToTestCp

  lazy val transformerPubsubBuildSettings =
    Seq(
      name := "snowplow-transformer-pubsub",
      Docker / packageName := "transformer-pubsub",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated",
      buildInfoKeys := List(name, version, description)
    ) ++ buildSettings ++ dynVerSettings ++ addExampleConfToTestCp

  lazy val transformerKafkaBuildSettings =
    Seq(
      name := "snowplow-transformer-kafka",
      Docker / packageName := "transformer-kafka",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated",
      buildInfoKeys := List(name, version, description)
    ) ++ buildSettings ++ dynVerSettings ++ addExampleConfToTestCp

}
