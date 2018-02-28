/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

lazy val loader = project.in(file("."))
  .settings(
    name := "snowplow-rdb-loader",
    version := "0.15.0-rc6",
    initialCommands := "import com.snowplowanalytics.snowplow.rdbloader._",
    mainClass in Compile := Some("com.snowplowanalytics.snowplow.rdbloader.Main")
  )
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings(name in shredder, version in shredder))
  .settings(BuildSettings.assemblySettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.scopt,
      Dependencies.scalaz7,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.scalaTracker,
      Dependencies.catsFree,
      Dependencies.circeYaml,
      Dependencies.circeGeneric,
      Dependencies.circeGenericExtra,
      Dependencies.manifest,

      Dependencies.postgres,
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

lazy val shredder = project.in(file("shredder"))
  .settings(
    name        := "snowplow-rdb-shredder",
    version     := "0.14.0-rc2",
    description := "Spark job to shred event and context JSONs from Snowplow enriched events",
    BuildSettings.oneJvmPerTestSetting // ensures that only CrossBatchDeduplicationSpec has a DuplicateStorage
  )
  .settings(BuildSettings.buildSettings)
  .settings(resolvers ++= Dependencies.resolutionRepos)
  .settings(BuildSettings.shredderAssemblySettings)
  .settings(BuildSettings.scalifySettings(name, version))
  .settings(
    libraryDependencies ++= Seq(
      // Java
      Dependencies.dynamodb,
      // Scala
      Dependencies.sparkCore,
      Dependencies.sparkSQL,
      Dependencies.scalaz7,
      Dependencies.scopt,
      Dependencies.commonEnrich,
      Dependencies.igluClient,
      Dependencies.igluCoreCirce,
      Dependencies.manifest,
      // Scala (test only)
      Dependencies.specs2
    )
  )
