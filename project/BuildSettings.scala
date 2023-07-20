/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

// SBT
import sbt._
import Keys._
import sbtbuildinfo.BuildInfoPlugin.autoImport._

// sbt-assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._

// sbt-native-packager
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._

import com.snowplowanalytics.snowplow.sbt.IgluSchemaPlugin.autoImport._

import scoverage.ScoverageKeys._

import sbtdynver.DynVerPlugin.autoImport._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

/**
 * Common settings-patterns for Snowplow apps and libraries.
 * To enable any of these you need to explicitly add Settings value to build.sbt
 */
object BuildSettings {

  /**
   * Common set of useful and restrictive compiler flags
   */
  lazy val buildSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.14",

    Compile / console / scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8"
    ),

    Compile / unmanagedResources += file("SNOWPLOW-LICENSE.md"),

    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full),

    resolvers ++= Dependencies.resolutionRepos
  ) ++ formattingSettings

  // sbt-assembly settings
  lazy val jarName = assembly / assemblyJarName := { name.value + "-" + version.value + ".jar" }

  lazy val assemblySettings = Seq(
    jarName,
    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("org", "apache", "commons", "logging", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "native-image", _ @ _*) => MergeStrategy.discard
      // case PathList("META-INF", _ @ _*) => MergeStrategy.discard    // Replaced with above for Stream Shredder
      case PathList("reference.conf", _ @ _*) => MergeStrategy.concat
      case PathList("codegen-resources", _ @ _*) => MergeStrategy.first // Part of AWS SDK v2
      case "mime.types" => MergeStrategy.first // Part of AWS SDK v2
      case "AUTHORS" => MergeStrategy.discard
      case PathList("org", "slf4j", "impl", _) => MergeStrategy.first
      case PathList("buildinfo", _) => MergeStrategy.first
      case x if x.contains("javax") => MergeStrategy.first
      case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first // http4s, 2.13 shim
      case x if x.endsWith("public-suffix-list.txt") => MergeStrategy.first
      case PathList("org", "apache", "log4j", _ @ _*) => MergeStrategy.first
      case PathList("SNOWPLOW-LICENSE.md") => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)

    }
  ) ++ (if (sys.env.get("SKIP_TEST").contains("true")) Seq(assembly / test := {}) else Seq())

  lazy val formattingSettings = Seq(
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false
  )

  lazy val transformerAssemblySettings = Seq(
    jarName,
    // Drop these jars
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "minlog-1.2.jar", // Otherwise causes conflicts with Kyro (which bundles it)
        "janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
        "commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
        "commons-beanutils-1.7.0.jar",      // "
        "hadoop-core-1.1.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
        "hadoop-tools-1.1.2.jar" // "
      )
      cp.filter { jar => excludes(jar.data.getName) }
    },

    assembly / assemblyMergeStrategy := {
      case "project.clj" => MergeStrategy.discard // Leiningen build files
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.endsWith(".html") => MergeStrategy.discard
      case x if x.endsWith("package-info.class") => MergeStrategy.first
      case x if x.endsWith("customization.config") => MergeStrategy.first
      case x if x.endsWith("examples-1.json") => MergeStrategy.first
      case x if x.endsWith("paginators-1.json") => MergeStrategy.first
      case x if x.endsWith("service-2.json") => MergeStrategy.first
      case x if x.endsWith("waiters-2.json") => MergeStrategy.first
      case x if x.endsWith("mime.types") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("com", "google", "common", _) => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", _) => MergeStrategy.first
      case PathList("scala", "annotation", "nowarn.class" | "nowarn$.class") => MergeStrategy.first // http4s, 2.13 shim
      case PathList("org", "apache", "commons", "logging", _ @ _*) => MergeStrategy.first
      case PathList("org", "slf4j", "impl", _) => MergeStrategy.first
      case "AUTHORS" => MergeStrategy.discard
      case PathList("com", "snowplowanalytics", "snowplow", "rdbloader", "generated", "ProjectMetadata.class" | "ProjectMetadata$.class") => MergeStrategy.first
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
    Test / testGrouping := (Test / definedTests).value map { test =>
      val forkOptions = ForkOptions()
        .withJavaHome(javaHome.value)
        .withOutputStrategy(outputStrategy.value)
        .withBootJars(Vector.empty)
        .withWorkingDirectory(Option(baseDirectory.value))
        .withConnectInput(connectInput.value)

      val runPolicy = Tests.SubProcess(forkOptions)
      Tests.Group(name = test.name, tests = Seq(test), runPolicy = runPolicy)
    }

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
  )

  lazy val additionalDockerSettings = Seq(
    Universal / mappings += file("SNOWPLOW-LICENSE.md") -> "/SNOWPLOW-LICENSE.md"
  )

  lazy val awsBuildSettings = {
    buildSettings
  }

  lazy val gcpBuildSettings = {
    buildSettings
  }
  
  lazy val azureBuildSettings = {
    buildSettings
  }

  lazy val commonBuildSettings = {
    Seq(
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.generated"
    ) ++ scoverageSettings ++ buildSettings
  }

  lazy val commonStreamTransformerBuildSettings = {
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
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val loaderBuildSettings = {
    buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val redshiftBuildSettings = {
    Seq(
      name := "snowplow-redshift-loader",
      Docker / packageName := "rdb-loader-redshift",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.redshift._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.redshift.Main")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val snowflakeBuildSettings = {
    Seq(
      name := "snowplow-snowflake-loader",
      Docker / packageName := "rdb-loader-snowflake",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.snowflake._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.snowflake.Main")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val databricksBuildSettings = {
    Seq(
      name := "snowplow-databricks-loader",
      Docker / packageName := "rdb-loader-databricks",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.databricks._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.databricks.Main"),
      Compile / unmanagedJars += file("DatabricksJDBC42.jar"),
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val transformerBatchBuildSettings = {
    Seq(
      name := "snowplow-transformer-batch",
      description := "Spark job to transform Snowplow enriched events into DB/query-friendly format",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.batch.generated",
      buildInfoKeys := List(name, version, description),
      assembly / assemblyShadeRules := Seq(
        ShadeRule.rename(
          // EMR has 0.1.42 installed
          "com.jcraft.jsch.**" -> "shadejsch.@1"
        ).inProject
      ),
      assembly / target := file("modules/transformer-batch/target/scala-2.12/assembled-jar"),
      BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
    ) ++ buildSettings ++ transformerAssemblySettings ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerKinesisBuildSettings = {
    Seq(
      name := "snowplow-transformer-kinesis",
      Docker / packageName := "transformer-kinesis",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated",
      buildInfoKeys := List(name, version, description),
    ) ++ buildSettings ++ assemblySettings ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerPubsubBuildSettings = {
    Seq(
      name := "snowplow-transformer-pubsub",
      Docker / packageName := "transformer-pubsub",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated",
      buildInfoKeys := List(name, version, description),
    ) ++ buildSettings ++ assemblySettings ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerKafkaBuildSettings =
    Seq(
      name := "snowplow-transformer-kafka",
      Docker / packageName := "transformer-kafka",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka.generated",
      buildInfoKeys := List(name, version, description)
    ) ++ buildSettings ++ assemblySettings ++ dynVerSettings ++ addExampleConfToTestCp



}
