val scalaVer = "2.13.16"
val sparkVer = "4.0.0"

name := "traffic-prediction"
version := "0.1-SNAPSHOT"
organization := "org.example"

ThisBuild / scalaVersion := scalaVer

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Wunused:imports")

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVer % "provided"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= sparkDependencies
  )

assembly / mainClass := Some("solution.Main")
assembly / assemblyJarName := "traffic-prediction.jar"

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated
Compile / runMain := Defaults.runMainTask(Compile / fullClasspath,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Spark program executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// Spark needs these on Java 17+ (spark-submit adds them itself)
Compile / run / javaOptions ++= Seq(
  "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
  "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
  "sun.nio.ch", "sun.nio.cs", "sun.security.action", "sun.util.calendar"
).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED") :+ "-Djdk.reflect.useDirectMethodHandle=false"

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(false)
