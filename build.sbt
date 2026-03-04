import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

// Match your Databricks cluster (Scala 2.13)
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / resolvers ++= Seq(
  // Required for edu.ucar/cdm-core which is hosted in Unidata repos, not Maven Central
  "Unidata All" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/"
)

lazy val root = (project in file("."))
  .settings(
    name := "zarr-spark",
    version := "0.1.3",
    organization := "com.epigene",
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    homepage := Some(url("https://github.com/antoinegaston/zarr-spark")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/antoinegaston/zarr-spark"),
        "scm:git:git@github.com:antoinegaston/zarr-spark.git"
      )
    ),
    developers := List(
      Developer("antoinegaston", "Antoine Gaston", "", url("https://github.com/antoinegaston"))
    ),
    publishMavenStyle := true,
    publishTo := sonatypePublishToBundle.value,
    sonatypeCredentialHost := "central.sonatype.com",
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
    ),
    // Exclude provided dependencies (Spark) from assembly; include others like zarr-java
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf"             => MergeStrategy.concat
      case x                            => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      // Use your Databricks Runtime’s Spark version (provided)
      "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",

      // Zarr Java (v2/v3 API used here)
      "dev.zarr" % "zarr-java" % "0.0.9",

      // Zstd codec for Zarr v3 string arrays
      "com.github.luben" % "zstd-jni" % "1.5.7-1",

      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.apache.spark" %% "spark-sql" % "3.5.0" % Test
    ),
    Test / parallelExecution := false,
    Test / fork := true
  )
