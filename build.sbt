organization := "com.github.mmolimar"
name := "hoolok"
version := "0.1.0-SNAPSHOT"

scmInfo := Some(ScmInfo(
  browseUrl = url("https://github.com/mmolimar/hoolok"),
  connection = "scm:git@github.com:mmolimar/hoolok.git",
  devConnection = "git@github.com:mmolimar/hoolok.git"
))

scalaVersion := "2.12.13"
libraryDependencies ++= {
  val sparkVersion = "3.1.1"
  val scalaTestVersion = "3.2.6"
  val circeVersion = "0.13.0"
  val circeYamlVersion = "0.13.1"
  val snakeYamlVersion = "1.28"
  val jinjavaVersion = "2.5.6"
  val reflectionsVersion = "0.9.12"
  val sparKJsonSchemaVersion = "0.6.3"
  val deequVersion = "1.1.0_spark-3.0-scala-2.12"
  val scalanlpVersion = "0.13.2"
  val deltaVersion = "0.8.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Compile,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Compile,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Compile,
    "org.apache.spark" %% "spark-avro" % sparkVersion % Compile,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Compile,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Compile,

    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.circe" %% "circe-yaml" % circeYamlVersion excludeAll ExclusionRule(organization = "org.yaml"),
    "org.yaml" % "snakeyaml" % snakeYamlVersion,
    "com.hubspot.jinjava" % "jinjava" % jinjavaVersion,
    "org.reflections" % "reflections" % reflectionsVersion,
    "org.zalando" %% "spark-json-schema" % sparKJsonSchemaVersion,
    "io.delta" %% "delta-core" % deltaVersion,
    "com.amazon.deequ" % "deequ" % deequVersion excludeAll(
      ExclusionRule(organization = "org.apache.spark"),
      ExclusionRule(organization = "org.scalanlp")
    ),
    "org.scalanlp" %% "breeze" % scalanlpVersion,

    "org.scalatest" %% "scalatest-wordspec" % scalaTestVersion % Test,
    "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % Test
  )
}

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value
(test in Test) := ((test in Test) dependsOn testScalastyle).value

sourceGenerators in Compile += {
  Def.task {
    val file = (sourceManaged in Compile).value / "com" / "github" / "mmolimar" / "hoolok" / "BuildInfo.scala"
    IO.write(
      file,
      s"""package com.github.mmolimar.hoolok
         |
         |private[hoolok] object BuildInfo {
         |  val version = "${version.value}"
         |}""".stripMargin
    )
    Seq(file)
  }.taskValue
}

val hoolokMainClass = "com.github.mmolimar.hoolok.JobRunner"
mainClass in(Compile, run) := Some(hoolokMainClass)
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated
fork in run := true

mainClass in assembly := Some(hoolokMainClass)
assemblyMergeStrategy in assembly := {
  case "module-info.class" => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.deduplicate
}

licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
