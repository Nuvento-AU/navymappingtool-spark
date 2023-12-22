ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"
assembly / assemblyJarName := "au.com.nuvento.navyerp-1.0.jar"
assembly /assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


val sparkVersion = "3.5.0"
val postgresVersion = "42.6.0"
val typesafeVersion = "1.4.2"
val playJson = "2.10.2"
val sparkExcel = "3.5.0_0.20.1"
lazy val root = (project in file("."))
  .settings(
    name := "spark-scala-role-mapping",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.postgresql" % "postgresql" % postgresVersion,
      "com.typesafe" % "config" % typesafeVersion,
      "com.typesafe.play" %% "play-json" % playJson,
      "org.apache.poi" % "poi" % "5.2.4",
      "org.apache.poi" % "poi-ooxml" % "5.2.4",
      "org.apache.poi" % "poi-ooxml-lite" % "5.2.4",
      "org.apache.hadoop" % "hadoop-azure" % "3.3.2",
      "com.microsoft.azure" % "azure-storage" % "8.6.6"
    )
  )
