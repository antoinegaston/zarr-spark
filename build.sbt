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
    publishMavenStyle := true,
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
