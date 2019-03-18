package com.gu.tableversions.examples

import java.net.URI
import java.sql.{Date, Timestamp}

import com.gu.tableversions.examples.DatePartitionedTableLoader.Pageview
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This example contains code that writes example event data to a table that has a single date partition.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  *
  * @param tableName the fully qualified table name that will be populated by this loader
  * @param tableLocation The location where the table data will be stored
  */
class DatePartitionedTableLoader(tableName: String, tableLocation: URI)(implicit val spark: SparkSession) {

  import spark.implicits._

  def initTable(): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '$tableLocation'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def pageviews(): Dataset[Pageview] =
    spark.table(tableName).as[Pageview]

  def insert(dataset: Dataset[Pageview]): Unit = {
    // Currently, this just uses the basic implementation of writing data to tables via Hive.
    // This will not do any versioning as-is - this is the implementation we need to replace
    // with new functionality in this project.
    dataset.write
      .mode(SaveMode.Overwrite)
      .insertInto(tableName)
  }

}

object DatePartitionedTableLoader {

  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, DateTime.timestampToUtcDate(timestamp))

  }

}
