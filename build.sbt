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

version in ThisBuild := "0.18.0"

lazy val root = project.in(file("."))
  .aggregate(common, loader, shredder)

lazy val common = project.in(file("modules/common"))
  .settings(Seq(
    name := "snowplow-rdb-loader-common"
  ))
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.buildSettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.slf4j,
      Dependencies.analyticsSdk,
      Dependencies.badrows,
      Dependencies.igluClient,
      Dependencies.scalaTracker,
      Dependencies.scalaTrackerEmit,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,
      Dependencies.circeLiteral,
      Dependencies.schemaDdl,
      Dependencies.specs2
    )
  )

lazy val loader = project.in(file("modules/loader"))
  .settings(
    name := "snowplow-rdb-loader",
    initialCommands := "import com.snowplowanalytics.snowplow.rdbloader._",
    Compile / mainClass := Some("com.snowplowanalytics.snowplow.rdbloader.Main")
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings(shredder / name, shredder / version))
  .settings(BuildSettings.assemblySettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    libraryDependencies ++= Seq(
      Dependencies.decline,
      Dependencies.scalaTracker,
      Dependencies.scalaTrackerEmit,
      Dependencies.circeYaml,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,
      Dependencies.circeLiteral,
      Dependencies.fs2,
      Dependencies.schemaDdl,

      Dependencies.redshift,
      Dependencies.redshiftSdk,
      Dependencies.s3,
      Dependencies.ssm,
      Dependencies.dynamodb,
      Dependencies.jSch,

      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
  .dependsOn(common)

lazy val shredder = project.in(file("modules/shredder"))
  .settings(
    name        := "snowplow-rdb-shredder",
    description := "Spark job to shred event and context JSONs from Snowplow enriched events",
    BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
  )
  .settings(BuildSettings.buildSettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(BuildSettings.shredderAssemblySettings)
  .settings(BuildSettings.scalifySettings(name, version))
  .settings(BuildSettings.dynamoDbSettings)
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.dynamodb,
      // Scala
      Dependencies.decline,
      Dependencies.eventsManifest,
      Dependencies.circeJawn,
      Dependencies.circeLiteral,
      Dependencies.schemaDdl,
      Dependencies.sparkCore,
      Dependencies.sparkSQL,
      Dependencies.igluCoreCirce,
      // Scala (test only)
      Dependencies.circeOptics,
      Dependencies.specs2,
      Dependencies.specs2ScalaCheck,
      Dependencies.scalaCheck
    )
  )
  .dependsOn(common)
