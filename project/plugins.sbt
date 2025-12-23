resolvers += Resolver.sbtPluginRepo("releases")
resolvers += Resolver.mavenCentral

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// Formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")

// Linting (Scalafix)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")
