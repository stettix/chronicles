import sbt._

object Dependencies {

  val sparkVersion = "2.3.2"

  lazy val sparkDependencies: Seq[ModuleID] =
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    )


}
