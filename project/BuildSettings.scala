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

// SBT
import sbt._
import Keys._

// sbt-assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._

// sbt-native-packager
import com.typesafe.sbt.packager.Keys.{daemonUser, maintainer}
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerVersion

import scoverage.ScoverageKeys._

// DynamoDB Local
import com.localytics.sbt.dynamodb.DynamoDBLocalKeys._

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
    scalaVersion := "2.12.12",

    Compile / console / scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8"
    ),

    addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.10" cross CrossVersion.binary)
  )

  // sbt-assembly settings
  lazy val jarName = assembly / assemblyJarName := { name.value + "-" + version.value + ".jar" }

  lazy val assemblySettings = Seq(
    jarName,

    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename(
        // EMR has 0.1.42 installed
        "com.jcraft.jsch.**" -> "shadejsch.@1"
      ).inAll
    ),

    assembly / assemblyMergeStrategy := {
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case PathList("reference.conf", _ @ _*) => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  ) ++ (if (sys.env.get("SKIP_TEST").contains("true")) Seq(test in assembly := {}) else Seq())

  lazy val shredderAssemblySettings = Seq(
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
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case PathList("com", "google", "common", _) => MergeStrategy.first
      case PathList("org", "apache", "spark", "unused", _) => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  ) ++ (if (sys.env.get("SKIP_TEST").contains("true")) Seq(test in assembly := {}) else Seq())

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := false,
    (test in Test) := {
      (coverageReport dependsOn (test in Test)).value
    }
  )

  /** Add `config` directory as a resource */
  lazy val addExampleConfToTestCp = Seq(
    unmanagedClasspath in Test += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )

  /**
   * Makes package (build) metadata available withing source code
   */
  def scalifySettings(shredderName: SettingKey[String], shredderVersion: SettingKey[String]) = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.rdbloader.generated
                       |object ProjectMetadata {
                       |  val version = "%s"
                       |  val name = "%s"
                       |  val organization = "%s"
                       |  val scalaVersion = "%s"
                       |
                       |  val shredderName = "%s"
                       |}
                       |""".stripMargin.format(
        version.value,name.value, organization.value, scalaVersion.value, shredderName.value, shredderVersion.value))
      Seq(file)
    }.taskValue
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

  lazy val dynamoDbSettings = Seq(
    startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value,
    test in Test := (test in Test).dependsOn(startDynamoDBLocal).value,
    testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).evaluated,
    testOptions in Test += dynamoDBLocalTestCleanup.value
  )

  lazy val dockerSettings = Seq(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.2.1",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    daemonUserUid in Docker := None,
    defaultLinuxInstallLocation in Docker := "/home/snowplow" // must be home directory of daemonUser
  )
}
