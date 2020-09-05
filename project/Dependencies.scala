import sbt._

object Dependencies {

  val sparkVersion = "2.4.3"

  val hadoopVersion = "2.6.5"

  lazy val hadoopDependencies: Seq[ModuleID] =
    Seq(
      "org.apache.hadoop" % "hadoop-common" %	hadoopVersion
    )

  lazy val sparkDependencies: Seq[ModuleID] =
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    )

  lazy val catsDependencies: Seq[ModuleID] =
    Seq(
      "org.typelevel" %% "cats-core" % "2.1.1",
      "org.typelevel" %% "cats-effect" % "2.1.4"
    )

  val fs2Version = "2.4.4"

  //val doobieVersion = "0.9.0"
  val doobieVersion = "0.7.0"

  lazy val doobieDependencies: Seq[ModuleID] =
    Seq(
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion,
      "org.tpolecat" %% "doobie-scalatest" % doobieVersion % "test,it"
    )

  lazy val scalatest = "org.scalatest" %% "scalatest" % "3.0.5"

  val circeVersion = "0.12.3"

  val pureConfigVersion = "0.13.0"

}
