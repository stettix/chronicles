package com.gu.tableversions.examples

import java.net.URI
import java.sql.{Date, Timestamp}

import com.gu.tableversions.examples.MultiPartitionTableLoader.AdImpression
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This example contains code that writes example event data to a table with multiple partition columns.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  *
  * @param tableName the fully qualified table name that will be populated by this loader
  * @param tableLocation The location where the table data will be stored
  */
class MultiPartitionTableLoader(tableName: String, tableLocation: URI)(implicit val spark: SparkSession) {

  import spark.implicits._

  def initTable(): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
                 |  `user_id` string,
                 |  `ad_id` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`impression_date` date, `processed_date` date)
                 |STORED AS parquet
                 |LOCATION '$tableLocation'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def insert(dataset: Dataset[AdImpression]): Unit = {
    // Currently, this just uses the basic implementation of writing data to tables via Hive.
    // This will not do any versioning as-is - this is the implementation we need to replace
    // with new functionality in this project.
    dataset.write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }

  def adImpressions(): Dataset[AdImpression] =
    spark.table(tableName).as[AdImpression]

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
