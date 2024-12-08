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
    
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
  )
