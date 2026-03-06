resolvers += Resolver.sbtPluginRepo("releases")
resolvers += Resolver.mavenCentral

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

// Formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.5")

// Linting (Scalafix)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.2.2")

// Maven Central publishing
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.11.3")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
