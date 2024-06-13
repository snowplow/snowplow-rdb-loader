logLevel := Level.Warn

addSbtPlugin("com.eed3si9n"          % "sbt-assembly"         % "0.14.9")
addSbtPlugin("org.typelevel"         % "sbt-tpolecat"         % "0.5.0")
addSbtPlugin("net.virtual-void"      % "sbt-dependency-graph" % "0.10.0-RC1")
addSbtPlugin("org.scoverage"         % "sbt-coveralls"        % "1.3.1")
addSbtPlugin("org.scoverage"         % "sbt-scoverage"        % "1.9.3")
addSbtPlugin("com.typesafe.sbt"      % "sbt-native-packager"  % "1.7.6")
addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo"        % "0.10.0")
addSbtPlugin("com.dwijnand"          % "sbt-dynver"           % "4.1.1")
addSbtPlugin("org.scalameta"         % "sbt-scalafmt"         % "2.4.6")
addSbtPlugin("com.snowplowanalytics" % "sbt-snowplow-release" % "0.3.2")
