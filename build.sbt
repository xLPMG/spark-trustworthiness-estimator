val scalaVersion = "2.12.20"
val sparkVersion = "3.5.3"

lazy val root = project
  .in(file("."))
  .settings(
    name := "spark-trustworthiness-estimator",
    version := "0.1.0-SNAPSHOT",

    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "com.databricks" %% "spark-xml" % "0.18.0",
    libraryDependencies += "org.apache.commons" % "commons-compress" % "1.21",

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )
