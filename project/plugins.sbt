logLevel := Level.Warn

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("com.localytics" % "sbt-dynamodb" % "2.0.3")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.20")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.1")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.7.6")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.10.0")