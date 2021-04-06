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

version in ThisBuild := "1.0.0-rc1"

lazy val root = project.in(file("."))
  .aggregate(common, aws, loader, shredder)

lazy val aws = project.in(file("modules/aws"))
  .settings(BuildSettings.buildSettings)
  .settings(
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    libraryDependencies ++= Seq(
      Dependencies.aws2s3,
      Dependencies.aws2sqs,
      Dependencies.fs2,
      Dependencies.catsRetry,
    )
  )
  .enablePlugins(BuildInfoPlugin)

lazy val common: Project = project.in(file("modules/common"))
  .settings(Seq(
    name := "snowplow-rdb-loader-common",
    buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.generated"
  ))
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.decline,
      Dependencies.badrows,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,
      Dependencies.circeLiteral,
      Dependencies.pureconfig,
      Dependencies.pureconfigCirce,
      Dependencies.schemaDdl,

      Dependencies.specs2,
      Dependencies.monocle,
      Dependencies.monocleMacro,
    )
  )
  .enablePlugins(BuildInfoPlugin)

lazy val loader = project.in(file("modules/loader"))
  .settings(
    name := "snowplow-rdb-loader",
    packageName in Docker := "snowplow/snowplow-rdb-loader",
    initialCommands := "import com.snowplowanalytics.snowplow.rdbloader._",
    Compile / mainClass := Some("com.snowplowanalytics.snowplow.rdbloader.Main")
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.dockerSettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    libraryDependencies ++= Seq(
      Dependencies.slf4j,
      Dependencies.redshift,
      Dependencies.redshiftSdk,
      Dependencies.ssm,
      Dependencies.dynamodb,
      Dependencies.jSch,
      Dependencies.sentry,

      Dependencies.scalaTracker,
      Dependencies.scalaTrackerEmit,
      Dependencies.fs2Blobstore,
      Dependencies.http4sClient,
      Dependencies.doobie,
      Dependencies.catsRetry,

      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
  .dependsOn(common, aws)
  .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)

lazy val shredder = project.in(file("modules/shredder"))
  .settings(
    name        := "snowplow-rdb-shredder",
    description := "Spark job to shred event and context JSONs from Snowplow enriched events",
    buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.shredder.batch.generated",
    buildInfoKeys := List(name, version, description),
    BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
  )
  .settings(BuildSettings.buildSettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(BuildSettings.shredderAssemblySettings)
  .settings(BuildSettings.dynamoDbSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.sqs,
      Dependencies.dynamodb,
      Dependencies.slf4j,
      // Scala
      Dependencies.eventsManifest,
      Dependencies.sparkCore,
      Dependencies.sparkSQL,
      // Scala (test only)
      Dependencies.circeOptics,
      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
  .dependsOn(common)
  .enablePlugins(BuildInfoPlugin)

lazy val streamShredder = project.in(file("modules/stream-shredder"))
  .settings(
    name        := "snowplow-rdb-stream-shredder",
    description := "Stream Shredding job",
    buildInfoPackage := "com.snowplowanalytics.snowplow.rdbloader.shredder.stream.generated",
    buildInfoKeys := List(name, version, description),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    scalacOptions.in(Compile) ~= {    // TODO: remove before production
      seq => seq.filterNot(_.contains("fatal-warnings"))
    }
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.assemblySettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(BuildSettings.dynamoDbSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.dynamodb,
      Dependencies.slf4j,
      // Scala
      Dependencies.log4cats,
      Dependencies.fs2Blobstore,
      Dependencies.fs2Io,
      Dependencies.fs2Aws,
      Dependencies.fs2AwsSqs,
      Dependencies.aws2kinesis,
      Dependencies.http4sClient,
      // Scala (test only)
      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
  .dependsOn(common, aws)
  .enablePlugins(BuildInfoPlugin)
