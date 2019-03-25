package com.gu.tableversions.examples

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.UserId
import com.gu.tableversions.core._
import com.gu.tableversions.examples.SnapshotTableLoader.User
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
  * every time we write to it (no partial updates).
  */
class SnapshotTableLoader(table: TableDefinition)(
    implicit tableVersions: TableVersions[IO],
    metastore: Metastore[IO],
    spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(): Unit = {
    // Create table schema in metastore
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    spark.sql(ddl)

    // Initialise version tracking for table
    tableVersions.init(table.name).unsafeRunSync()
  }

  def users(): Dataset[User] =
    spark.table(table.name.fullyQualifiedName).as[User]

  def insert(dataset: Dataset[User], userId: UserId, message: String): Unit = {
    import VersionedDataset._

    val (latestVersion, metastoreChanges) = dataset.versionedInsertInto(table, userId, message)

    logger.info(s"Updated table $table, new version details:\n$latestVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

}

object SnapshotTableLoader {

  case class User(id: String, name: String, email: String)

}
