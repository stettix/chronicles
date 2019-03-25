package com.gu.tableversions.spark

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import scala.util.Try

/**
  * Base trait for ScalaTest tests that use a local Spark context with Hive support enabled.
  * This allows testing of code that uses a Hive Metastore e.g. for creating and populating tables.
  *
  * This also provides temporary directories to which test data can be copied, so that Spark code
  * that reads files can be fully tested.
  *
  * Note that this trait uses a single SparkContext and Hive Metastore for all tests, to avoid
  * the overhead of creating one for each test case. This trait will drop and create the default database
  * and clear any content of the input directory before each test, but you may want to clean up further state
  * yourself in derived specs.
  *
  * Alternatively, if this causes a problem, you can work around
  * it by using the [[org.scalatest.OneInstancePerTest]] trait.
  */
trait SparkHiveSuite extends BeforeAndAfterAll with BeforeAndAfterEach with LazyLogging {
  self: Suite =>

  // Need to do this to let Hive load the JDBC driver for the embedded Derby instance
  // used for the metastore DB.  o(>< )o
  // See https://stackoverflow.com/questions/48008343/sbt-test-does-not-work-for-spark-test
  System.setSecurityManager(null)

  val schema: String = "sparkhivesuite"

  private val rootDir: Path = Files.createDirectories(Paths.get(URI.create(s"file:///tmp/${UUID.randomUUID.toString}")))

  val inputDir: Path = rootDir.resolve("input/")
  logger.info(s"Creating SparkHiveSuite in $rootDir")

  val tableDir: Path = rootDir.resolve("table/")
  val tableUri = tableDir.toUri

  val localMetastorePath: String = new File(rootDir.toFile, "metastore").getCanonicalPath
  val localWarehousePath: String = new File(rootDir.toFile, "warehouse").getCanonicalPath

  def jobConfig: Map[String, String] = Map(
    "spark.network.timeout" -> "2400",
    "spark.shuffle.io.maxRetries" -> "15",
    "spark.driver.maxResultSize" -> "0",
    "spark.shuffle.consolidateFiles" -> "true",
    "spark.task.maxFailures" -> "3",
    SQLConf.ORC_IMPLEMENTATION.key -> "native",
    "mapred.input.dir.recursive" -> "true",
    "mapreduce.input.fileinputformat.input.dir.recursive" -> "true"
  )

  // set deregister=false to avoid race conditions when shutting down and reloading driver
  val connectionUrl = s"jdbc:derby:;databaseName=$localMetastorePath;create=true;deregister=false"

  lazy val spark: SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
      .set("spark.ui.enabled", "false")
      .setAppName(getClass.getSimpleName)
      .set("hive.exec.dynamic.partition", "true") // allow dynamic partitioning (default=false prior to Hive 0.9.0)
      .set("hive.exec.dynamic.partition.mode", "nonstrict") // setting required because the only partition is dynamic
      .set("javax.jdo.option.ConnectionURL", connectionUrl)
      .set("spark.sql.warehouse.dir", localWarehousePath)
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.orc.impl", "native")
      .set("spark.sql.shuffle.partitions", "8") // For speeding up tests. The default is 200, see https://spark.apache.org/docs/latest/sql-programming-guide.html#other-configuration-options

    jobConfig.foreach { case (key, value) => conf.set(key, value) }

    SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
  }

  implicit lazy val ss: SparkSession = spark

  override def afterAll(): Unit = {
    Try(FileUtils.deleteDirectory(rootDir.toFile))
    Try(spark.sparkContext.stop)
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    Files.createDirectory(inputDir)
    Files.createDirectory(tableDir)
    spark.sql(s"create database $schema").take(1)
    ()
  }

  override protected def afterEach(): Unit = {
    spark.sql(s"drop database $schema cascade")
    Try(FileUtils.deleteDirectory(inputDir.toFile))
    Try(FileUtils.deleteDirectory(tableDir.toFile))
    ()
  }

  def resolveTablePath(childPath: String): URI = new URI(tableUri.toString + "/" + childPath)

}
