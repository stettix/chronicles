import sbt._

object Dependencies {

  val sparkVersion = "2.4.3"

  lazy val sparkDependencies: Seq[ModuleID] =
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    )

  lazy val catsDependencies: Seq[ModuleID] =
    Seq(
      "org.typelevel" %% "cats-core" % "1.6.1",
      "org.typelevel" %% "cats-effect" % "1.3.1"
    )

  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

  val circeVersion = "0.11.1"

}
