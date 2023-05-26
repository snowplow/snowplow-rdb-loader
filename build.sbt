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

/**
 * Inspired by the same issue in the Iglu Server, see commit https://github.com/snowplow/iglu-server/pull/128/commits/d4a0940c12181dd40bc5cdc2249f2c84c5296353
 * 
 * Currently we have some libs that depend on circe 0.14.x and some that depend on 0.13.x.
 * These reported binary incompatibilities can only be removed once we have bumped cats-effect to version 3.
 * For now, we ignore the reported binary incompatibilities because testing shows it is safe.
 */
ThisBuild / libraryDependencySchemes ++= Seq(
  "io.circe" %% "circe-core" % "always",
  "io.circe" %% "circe-generic" % "always",
  "io.circe" %% "circe-parser" % "always",
  "io.circe" %% "circe-jawn" % "always",
)

lazy val root = project.in(file("."))
  .aggregate(
    aws,
    gcp,
    azure,
    common,
    commonTransformerStream,
    loader,
    databricksLoader,
    redshiftLoader,
    snowflakeLoader,
    transformerBatch,
    transformerKinesis,
    transformerPubsub,
    transformerKafka 
  )

lazy val common: Project = project
  .in(file("modules/common"))
  .settings(BuildSettings.commonBuildSettings)
  .settings(libraryDependencies ++= Dependencies.commonDependencies)
  .enablePlugins(BuildInfoPlugin)

lazy val aws: Project = project
  .in(file("modules/aws"))
  .settings(BuildSettings.awsBuildSettings)
  .settings(libraryDependencies ++= Dependencies.awsDependencies)
  .dependsOn(common % "compile->compile;test->test")
  .enablePlugins(BuildInfoPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpBuildSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .dependsOn(common % "compile->compile;test->test")
  .enablePlugins(BuildInfoPlugin)

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureBuildSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .dependsOn(common % "compile->compile;test->test")
  .enablePlugins(BuildInfoPlugin)

lazy val commonTransformerStream = project
  .in(file("modules/common-transformer-stream"))
  .settings(BuildSettings.commonStreamTransformerBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.commonStreamTransformerDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(common % "compile->compile;test->test")
  .enablePlugins(BuildInfoPlugin)

lazy val loader = project
  .in(file("modules/loader"))
  .settings(BuildSettings.loaderBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.loaderDependencies)
  .dependsOn(aws % "compile->compile;test->test;runtime->runtime", gcp % "compile->compile;test->test")

lazy val redshiftLoader = project
  .in(file("modules/redshift-loader"))
  .settings(BuildSettings.redshiftBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.redshiftDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val redshiftLoaderDistroless = project
  .in(file("modules/distroless/redshift-loader"))
  .settings(sourceDirectory := (redshiftLoader / sourceDirectory).value)
  .settings(BuildSettings.redshiftBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.redshiftDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val snowflakeLoader = project
  .in(file("modules/snowflake-loader"))
  .settings(BuildSettings.snowflakeBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.snowflakeDependencies)
  .dependsOn(common % "compile->compile;test->test",loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val snowflakeLoaderDistroless = project
  .in(file("modules/distroless/snowflake-loader"))
  .settings(sourceDirectory := (snowflakeLoader / sourceDirectory).value)
  .settings(BuildSettings.snowflakeBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.snowflakeDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val databricksLoader = project
  .in(file("modules/databricks-loader"))
  .settings(BuildSettings.databricksBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val databricksLoaderDistroless = project
  .in(file("modules/distroless/databricks-loader"))
  .settings(sourceDirectory := (databricksLoader / sourceDirectory).value)
  .settings(BuildSettings.databricksBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val transformerBatch = project
  .in(file("modules/transformer-batch"))
  .settings(BuildSettings.transformerBatchBuildSettings)
  .settings(libraryDependencies ++= Dependencies.batchTransformerDependencies)
  .dependsOn(common % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(BuildInfoPlugin)

lazy val transformerKinesis = project
  .in(file("modules/transformer-kinesis"))
  .settings(BuildSettings.transformerKinesisBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKinesisDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", aws % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val transformerKinesisDistroless = project
  .in(file("modules/distroless/transformer-kinesis"))
  .settings(sourceDirectory := (transformerKinesis / sourceDirectory).value)
  .settings(BuildSettings.transformerKinesisBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKinesisDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", aws % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val transformerPubsub = project
  .in(file("modules/transformer-pubsub"))
  .settings(BuildSettings.transformerPubsubBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerPubsubDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", gcp % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val transformerPubsubDistroless = project
  .in(file("modules/distroless/transformer-pubsub"))
  .settings(sourceDirectory := (transformerPubsub / sourceDirectory).value)
  .settings(BuildSettings.transformerPubsubBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerPubsubDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", gcp % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val transformerKafka = project
  .in(file("modules/transformer-kafka"))
  .settings(BuildSettings.transformerKafkaBuildSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKafkaDependencies)
  .settings(excludeDependencies ++= Dependencies.commonStreamTransformerExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", azure % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

