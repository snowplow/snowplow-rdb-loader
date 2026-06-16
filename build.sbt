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

ThisBuild / scalaVersion := "2.12.21"

// zstd-jni uses a non-standard version format (e.g. 1.4.9-1 vs 1.5.2-1). semver-spec tells sbt
// to treat the minor bump as backward-compatible rather than flagging a binary incompatibility.
ThisBuild / libraryDependencySchemes += "com.github.luben" % "zstd-jni" % "semver-spec"

lazy val root = project
  .in(file("."))
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
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(common % "compile->compile;test->test")
  .enablePlugins(BuildInfoPlugin)

lazy val loader = project
  .in(file("modules/loader"))
  .settings(BuildSettings.loaderBuildSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.loaderDependencies)
  .dependsOn(
    aws   % "compile->compile;test->test;runtime->runtime",
    gcp   % "compile->compile;test->test",
    azure % "compile->compile;test->test"
  )

lazy val redshiftLoader = project
  .in(file("modules/redshift-loader"))
  .settings(BuildSettings.redshiftBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.redshiftDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val redshiftLoaderDistroless = project
  .in(file("modules/distroless/redshift-loader"))
  .settings(sourceDirectory := (redshiftLoader / sourceDirectory).value)
  .settings(BuildSettings.redshiftBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.redshiftDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val snowflakeLoader = project
  .in(file("modules/snowflake-loader"))
  .settings(BuildSettings.snowflakeBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.snowflakeDependencies)
  .dependsOn(common % "compile->compile;test->test", loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val snowflakeLoaderDistroless = project
  .in(file("modules/distroless/snowflake-loader"))
  .settings(sourceDirectory := (snowflakeLoader / sourceDirectory).value)
  .settings(BuildSettings.snowflakeBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.snowflakeDependencies)
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val databricksLoader = project
  .in(file("modules/databricks-loader"))
  .settings(BuildSettings.databricksBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .dependsOn(loader % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val databricksLoaderDistroless = project
  .in(file("modules/distroless/databricks-loader"))
  .settings(sourceDirectory := (databricksLoader / sourceDirectory).value)
  .settings(BuildSettings.databricksDistrolessBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
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
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKinesisDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", aws % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val transformerKinesisDistroless = project
  .in(file("modules/distroless/transformer-kinesis"))
  .settings(sourceDirectory := (transformerKinesis / sourceDirectory).value)
  .settings(BuildSettings.transformerKinesisBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKinesisDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", aws % "compile->compile;test->test;runtime->runtime")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val transformerPubsub = project
  .in(file("modules/transformer-pubsub"))
  .settings(BuildSettings.transformerPubsubBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerPubsubDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", gcp % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val transformerPubsubDistroless = project
  .in(file("modules/distroless/transformer-pubsub"))
  .settings(sourceDirectory := (transformerPubsub / sourceDirectory).value)
  .settings(BuildSettings.transformerPubsubBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerPubsubDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", gcp % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)

lazy val transformerKafka = project
  .in(file("modules/transformer-kafka"))
  .settings(BuildSettings.transformerKafkaBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKafkaDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(
    commonTransformerStream % "compile->compile;test->test;runtime->runtime",
    azure                   % "compile->compile;test->test;runtime->runtime"
  )
  .enablePlugins(JavaAppPackaging, SnowplowDockerPlugin, BuildInfoPlugin)

lazy val transformerKafkaDistroless = project
  .in(file("modules/distroless/transformer-kafka"))
  .settings(sourceDirectory := (transformerKafka / sourceDirectory).value)
  .settings(BuildSettings.transformerKafkaBuildSettings)
  .settings(BuildSettings.additionalDockerSettings)
  .settings(addCompilerPlugin(Dependencies.betterMonadicFor))
  .settings(libraryDependencies ++= Dependencies.transformerKafkaDependencies)
  .settings(excludeDependencies ++= Dependencies.commonExclusions)
  .dependsOn(commonTransformerStream % "compile->compile;test->test;runtime->runtime", azure % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, SnowplowDistrolessDockerPlugin, BuildInfoPlugin)
