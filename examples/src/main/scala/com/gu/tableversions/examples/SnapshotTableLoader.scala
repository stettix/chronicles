package com.gu.tableversions.examples

import java.net.URI

import com.gu.tableversions.examples.SnapshotTableLoader.User
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
  * every time we write to it (no partial updates).
  *
  * @param tableName The fully qualified table name
  * @param tableLocation The location where the table data will be stored
  */
class SnapshotTableLoader(tableName: String, tableLocation: URI)(implicit val spark: SparkSession) {

  import spark.implicits._

  def initTable(): Unit = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '$tableLocation'
    """.stripMargin

    spark.sql(ddl)
    ()
  }

  def insert(dataset: Dataset[User]): Unit = {
    // Currently, this just uses the basic implementation of writing data to tables via Hive.
    // This will not do any versioning as-is - this is the implementation we need to replace
    // with new functionality in this project.
    dataset.write.mode(SaveMode.Overwrite).parquet(tableLocation.toString)
    spark.sql(s"REFRESH TABLE $tableName")
    ()
  }

  def users(): Dataset[User] =
    spark.table(tableName).as[User]

}

object SnapshotTableLoader {

  case class User(id: String, name: String, email: String)

}
