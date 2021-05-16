organization := "com.github.mmolimar"
name := "hoolok"
version := "0.1.0-SNAPSHOT"

scmInfo := Some(ScmInfo(
  browseUrl = url("https://github.com/mmolimar/hoolok"),
  connection = "scm:git@github.com:mmolimar/hoolok.git",
  devConnection = "git@github.com:mmolimar/hoolok.git"
))

scalaVersion := "2.12.12"
libraryDependencies ++= {
  val sparkVersion = "3.1.1"
  val circeVersion = "0.13.0"
  val circeYamlVersion = "0.13.1"
  val snakeYamlVersion = "1.28"
  val jinjavaVersion = "2.5.7"
  val reflectionsVersion = "0.9.12"
  val sparkJsonSchemaVersion = "0.6.3"
  val abrisVersion = "4.2.0"
  val deequVersion = "1.2.2-spark-3.0"
  val deltaVersion = "0.8.0"
  val javaFakerVersion = "1.0.2"

  val scalaTestVersion = "3.2.9"
  val embeddedKafkaVersion = "2.3.1"
  val schemaRegistryVersion = "5.3.4"
  val mockServer = "5.11.2"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-avro" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,

    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.circe" %% "circe-yaml" % circeYamlVersion excludeAll ExclusionRule(organization = "org.yaml"),
    "org.yaml" % "snakeyaml" % snakeYamlVersion,
    "com.hubspot.jinjava" % "jinjava" % jinjavaVersion,
    "org.reflections" % "reflections" % reflectionsVersion,
    "org.zalando" %% "spark-json-schema" % sparkJsonSchemaVersion,
    "za.co.absa" %% "abris" % abrisVersion,
    "io.delta" %% "delta-core" % deltaVersion,
    "com.github.javafaker" % "javafaker" % javaFakerVersion excludeAll(
      ExclusionRule(organization = "org.yaml"),
      ExclusionRule(organization = "org.reflections")
    ),
    "com.amazon.deequ" % "deequ" % deequVersion excludeAll ExclusionRule(organization = "org.apache.spark"),

    "org.mock-server" % "mockserver" % mockServer % "it,test",
    "org.scalatest" %% "scalatest-wordspec" % scalaTestVersion % "it,test",
    "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion % "it,test",
    "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion % "it,test",
    "io.confluent" % "kafka-schema-registry" % schemaRegistryVersion % "it,test"
  )
}

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := Compile / scalastyle
Compile / compile := (Compile / compile dependsOn compileScalastyle).value

Compile / sourceGenerators += {
  Def.task {
    val file = (Compile / sourceManaged).value / "com" / "github" / "mmolimar" / "hoolok" / "BuildInfo.scala"
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

resolvers ++= Seq(
  "Confluent Maven Repo" at "https://packages.confluent.io/maven/",
  "jitpack" at "https://jitpack.io",
  Resolver.sonatypeRepo("snapshots"),
  Resolver.mavenLocal
)

val hoolokMainClass = "com.github.mmolimar.hoolok.JobRunner"
Compile / run / mainClass := Some(hoolokMainClass)
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
fork / run := true

assembly / mainClass := Some(hoolokMainClass)
assembly / assemblyMergeStrategy := {
  case "plugin.xml" => MergeStrategy.discard
  case "git.properties" => MergeStrategy.discard
  case "jetty-dir.css" => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case "NOTICE" => MergeStrategy.discard
  case "MANIFEST.MF" => MergeStrategy.discard
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList(ps@_*) if ps.last contains "LICENSE" => MergeStrategy.first
  case PathList("org", "apache", _*) => MergeStrategy.first
  case PathList("javax", _*) => MergeStrategy.first
  case PathList("com", "sun", _*) => MergeStrategy.first
  case PathList("org", "aopalliance", _*) => MergeStrategy.first
  case PathList("net", "jcip", "annotations", _*) => MergeStrategy.first
  case PathList("edu", "umd", "cs", "findbugs", _*) => MergeStrategy.first
  case _ => MergeStrategy.deduplicate
}

licenses := Seq("Apache License, Version 2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))

import TestSettings._

lazy val root = (project in file("."))
  .configs(TestSettings.IntegrationTest)
  .settings(hoolokSettings: _*)
