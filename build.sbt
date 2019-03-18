import Shared._
import Dependencies._

import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._

ThisBuild / organization := "com.gu"
ThisBuild / name := "table-versions"
ThisBuild / description := "Version control for your Big Data!"

lazy val commonSettings = Seq(
  version := "0.0.1",
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
  scalaVersion := "2.11.12",
  scalacOptions ++= scala211CompilerFlags,
  scalafmtOnCompile := true,
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
  ),
  resolvers += Resolver.sonatypeRepo("releases"),
  cancelable in Global := true,
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
) ++ testSettings

lazy val testSettings = Defaults.itSettings ++ Seq(
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  parallelExecution in IntegrationTest := false,
  fork in IntegrationTest := true
)

lazy val root = (project in file("."))
  .aggregate(core, metastore, spark, dynamodb, cli, examples)
  .settings(commonSettings)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)

lazy val metastore = project
  .in(file("metastore"))
  .settings(commonSettings)
  .dependsOn(core)

lazy val cli = project
  .in(file("cli"))
  .settings(commonSettings)
  .dependsOn(core, metastore, dynamodb)

lazy val spark = project
  .in(file("spark"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % "2.6",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      "log4j" % "log4j" % "1.2.17"
    ) ++ sparkDependencies)
  .dependsOn(core, metastore)

lazy val dynamodb = project
  .in(file("dynamodb"))
  .settings(commonSettings)
  .dependsOn(core)

lazy val examples = project
  .in(file("examples"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= sparkDependencies
  )
  .settings(
    parallelExecution in Test := false
  )
  .dependsOn(core, metastore, dynamodb, spark % "compile->compile;test->test")
