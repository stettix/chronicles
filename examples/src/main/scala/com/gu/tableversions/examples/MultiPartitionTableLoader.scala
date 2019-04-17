package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}
import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core.{TableDefinition, TableVersions}
import com.gu.tableversions.examples.MultiPartitionTableLoader.AdImpression
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This example contains code that writes example event data to a table with multiple partition columns.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  */
class MultiPartitionTableLoader(table: TableDefinition)(
    implicit tableVersions: TableVersions[IO],
    metastore: Metastore[IO],
    spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(userId: UserId, message: UpdateMessage): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `user_id` string,
                 |  `ad_id` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`impression_date` date, `processed_date` date)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    spark.sql(ddl)
    ()

    // Initialise version tracking for table
    tableVersions.init(table.name, isSnapshot = false, userId, message, Instant.now()).unsafeRunSync()
  }

  def insert(dataset: Dataset[AdImpression], userId: UserId, message: String): Unit = {
    import VersionedDataset._

    val (latestVersion, metastoreChanges) = dataset.versionedInsertInto(table, userId, message)

    logger.info(s"Updated table $table, new version details:\n$latestVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

  def adImpressions(): Dataset[AdImpression] =
    spark.table(table.name.fullyQualifiedName).as[AdImpression]

}

object MultiPartitionTableLoader {

  case class AdImpression(
      user_id: String,
      ad_id: String,
      timestamp: Timestamp,
      impression_date: Date,
      processed_date: Date)

  object AdImpression {

    def apply(userId: String, adId: String, timestamp: Timestamp, processedDate: Date): AdImpression =
      AdImpression(userId, adId, timestamp, DateTime.timestampToUtcDate(timestamp), processedDate)
  }

}
