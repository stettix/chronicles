package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.UserId
import com.gu.tableversions.core._
import com.gu.tableversions.examples.DatePartitionedTableLoader.Pageview
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This example contains code that writes example event data to a table that has a single date partition.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  */
class DatePartitionedTableLoader(table: TableDefinition)(
    implicit tableVersions: TableVersions[IO],
    metastore: Metastore[IO],
    spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(): Unit = {
    // Create table schema in metastore
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    spark.sql(ddl)

    // Initialise version tracking for table
    tableVersions.init(table.name).unsafeRunSync()
  }

  def pageviews(): Dataset[Pageview] =
    spark.table(table.name.fullyQualifiedName).as[Pageview]

  def insert(dataset: Dataset[Pageview], userId: UserId, message: String): Unit = {
    import VersionedDataset._

    val (latestVersion, metastoreChanges) = dataset.versionedInsertInto(table, userId, message)

    logger.info(s"Updated table $table, new version details:\n$latestVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

}

object DatePartitionedTableLoader {

  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, DateTime.timestampToUtcDate(timestamp))

  }

}
