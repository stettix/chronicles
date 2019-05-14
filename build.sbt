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
    scalatest % Test,
    "org.scalacheck" %% "scalacheck" % "1.14.0" % Test
  ),
  resolvers += Resolver.sonatypeRepo("releases"),
  cancelable in Global := true,
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
) ++ testSettings ++ assemblySettings

lazy val testSettings = Defaults.itSettings ++ Seq(
  test in assembly := {},
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  parallelExecution in IntegrationTest := false,
  fork in IntegrationTest := true
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value}.jar"
)

lazy val root = (project in file("."))
  .aggregate(`table-versions-core`,
             `table-versions-spark`,
             `table-versions-glue`,
             `table-versions-cli`,
             `table-versions-examples`)
  .settings(commonSettings)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val `table-versions-core` = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= catsDependencies)

lazy val `table-versions-cli` = project
  .in(file("cli"))
  .settings(commonSettings)
  .dependsOn(`table-versions-core`)

lazy val `table-versions-spark` = project
  .in(file("spark"))
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "commons-io" % "commons-io" % "2.6",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "log4j" % "log4j" % "1.2.17"
  ) ++ sparkDependencies)
  .settings(parallelExecution in Test := false)
  .settings(fork in Test := true)
  .dependsOn(`table-versions-core` % "compile->compile;test->test")

lazy val `table-versions-glue` = project
  .in(file("glue"))
  .settings(commonSettings)
  .configs(IntegrationTest)
  .settings(libraryDependencies ++= Seq(
    "commons-io" % "commons-io" % "2.6",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "log4j" % "log4j" % "1.2.17",
    "com.amazonaws" % "aws-java-sdk-glue" % "1.11.538",
    scalatest % IntegrationTest
  ))
  .settings(parallelExecution in Test := false)
  .dependsOn(`table-versions-core` % "compile->compile;test->test;it->test")

lazy val `table-versions-examples` = project
  .in(file("examples"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= sparkDependencies
  )
  .settings(parallelExecution in Test := false)
  .settings(fork in Test := true)
  .dependsOn(`table-versions-core`, `table-versions-spark` % "compile->compile;test->test")
