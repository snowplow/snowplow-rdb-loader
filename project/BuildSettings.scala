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
import com.typesafe.sbt.packager.docker.DockerPermissionStrategy
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerVersion

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

  lazy val dockerSettingsFocal = Seq(
    dockerBaseImage := "adoptopenjdk:11-jre-hotspot-focal",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    Docker / daemonUser := "daemon",
    Docker / daemonUserUid := None,
    Docker / defaultLinuxInstallLocation := "/opt/snowplow"
  )

  lazy val dockerSettingsDistroless = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "gcr.io/distroless/java11-debian11:nonroot",
    Docker / daemonUser := "nonroot",
    Docker / daemonGroup := "nonroot",
    dockerRepository := Some("snowplow"),
    Docker / daemonUserUid := None,
    Docker / defaultLinuxInstallLocation := "/home/snowplow",
    dockerEntrypoint := Seq("java", "-jar",s"/home/snowplow/lib/${(packageJavaLauncherJar / artifactPath).value.getName}"),
    dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
    dockerAlias := dockerAlias.value.copy(tag = dockerAlias.value.tag.map(t => s"$t-distroless")),
    dockerUpdateLatest := false
  )

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
  )

  lazy val awsBuildSettings = {
    buildSettings
  }

  lazy val gcpBuildSettings = {
    buildSettings
  }

  lazy val commonBuildSettings = {
    Seq(
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.generated"
    ) ++ scoverageSettings ++ buildSettings
  }

  lazy val commonStreamTransformerBuildSettings = {
    buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val loaderBuildSettings = {
    buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dynVerSettings
  }

  lazy val redshiftBuildSettings = {
    Seq(
      name := "snowplow-redshift-loader",
      Docker / packageName := "snowplow/rdb-loader-redshift",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.redshift._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.redshift.Main")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dockerSettingsFocal ++ dynVerSettings
  }

  lazy val redshiftDistrolessBuildSettings = redshiftBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val snowflakeBuildSettings = {
    Seq(
      name := "snowplow-snowflake-loader",
      Docker / packageName := "snowplow/rdb-loader-snowflake",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.snowflake._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.snowflake.Main")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dockerSettingsFocal ++ dynVerSettings
  }

  lazy val snowflakeDistrolessBuildSettings = snowflakeBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val databricksBuildSettings = {
    Seq(
      name := "snowplow-databricks-loader",
      Docker / packageName := "snowplow/rdb-loader-databricks",
      initialCommands := "import com.snowplowanalytics.snowplow.loader.databricks._",
      Compile / mainClass := Some("com.snowplowanalytics.snowplow.loader.databricks.Main"),
      Compile / unmanagedJars += file("DatabricksJDBC42.jar")
    ) ++ buildSettings ++ addExampleConfToTestCp ++ assemblySettings ++ dockerSettingsFocal ++ dynVerSettings
  }

  lazy val databricksDistrolessBuildSettings = databricksBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

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
      BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
    ) ++ buildSettings ++ transformerAssemblySettings ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerKinesisBuildSettings = {
    Seq(
      name := "snowplow-transformer-kinesis",
      Docker / packageName := "snowplow/transformer-kinesis",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kinesis.generated",
      buildInfoKeys := List(name, version, description),
    ) ++ buildSettings ++ assemblySettings ++ dockerSettingsFocal ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerKinesisDistrolessBuildSettings = transformerKinesisBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val transformerPubsubBuildSettings = {
    Seq(
      name := "snowplow-transformer-pubsub",
      Docker / packageName := "snowplow/transformer-pubsub",
      buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.transformer.stream.pubsub.generated",
      buildInfoKeys := List(name, version, description),
    ) ++ buildSettings ++ assemblySettings ++ dockerSettingsFocal ++ dynVerSettings ++ addExampleConfToTestCp
  }

  lazy val transformerPubsubDistrolessBuildSettings = transformerPubsubBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

}
