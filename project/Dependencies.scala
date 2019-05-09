import sbt._

object Dependencies {

  val sparkVersion = "2.3.2"

  lazy val sparkDependencies: Seq[ModuleID] =
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    )

  lazy val catsDependencies: Seq[ModuleID] =
    Seq(
      "org.typelevel" %% "cats-core" % "1.5.0",
      "org.typelevel" %% "cats-effect" % "1.2.0"
    )

  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

  val circeVersion = "0.10.0"

}
