val scalaVersion = "2.12.20"
val sparkVersion = "3.5.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "spark-trustworthiness-estimator",
    version := "0.1.0",

    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion,
    libraryDependencies += "com.databricks" %% "spark-xml" % "0.18.0",
    libraryDependencies += "org.apache.commons" % "commons-compress" % "1.21",

    libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.15.0",
    libraryDependencies += "org.apache.parquet" % "parquet-encoding" % "1.15.0",
    libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.15.0",
    libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.15.0",

    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "2.0.0",

    // Logging dependencies
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.13",
    libraryDependencies += "ch.qos.logback" % "logback-core" % "1.2.13",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.33",

    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
  )
