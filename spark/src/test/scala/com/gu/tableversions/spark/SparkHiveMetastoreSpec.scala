package com.gu.tableversions.spark

import java.net.URI

import cats.effect.IO
import cats.syntax.functor._
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.MetastoreSpec
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import SparkHiveMetastore._

class SparkHiveMetastoreSpec extends FlatSpec with Matchers with SparkHiveSuite with MetastoreSpec with PropertyChecks {

  val snapshotTable =
    TableDefinition(TableName(schema, "users"), resolveTablePath("users"), PartitionSchema.snapshot)

  val partitionedTable = TableDefinition(TableName(schema, "clicks"),
                                         resolveTablePath("clicks"),
                                         PartitionSchema(List(PartitionColumn("date"))))
  //
  // Common specs
  //

  "A metastore implementation" should behave like metastoreWithSnapshotSupport(IO { new SparkHiveMetastore },
                                                                               initSnapshotTable(snapshotTable),
                                                                               snapshotTable.name)

  it should behave like metastoreWithPartitionsSupport(IO { new SparkHiveMetastore },
                                                       initPartitionedTable(partitionedTable),
                                                       partitionedTable.name)

  //
  // Tests specific to the Spark/Hive implementation
  //

  "Parsing a valid partition string" should "produce the expected values" in {
    val testData = Table(
      ("partitionStr", "expected"),
      ("date=2019-01-31", Partition(List(ColumnValue(PartitionColumn("date"), "2019-01-31")))),
      ("event_date=2019-01-30/processed_date=2019-01-31",
       Partition(
         List(ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
              ColumnValue(PartitionColumn("processed_date"), "2019-01-31")))),
      ("year=2019/month=01/day=31",
       Partition(
         List(ColumnValue(PartitionColumn("year"), "2019"),
              ColumnValue(PartitionColumn("month"), "01"),
              ColumnValue(PartitionColumn("day"), "31"))))
    )

    forAll(testData) { (partitionStr: String, expected: Partition) =>
      SparkHiveMetastore.parsePartition(partitionStr) shouldBe expected
    }
  }

  "Parsing an invalid partition string" should "throw an exception" in {
    // format: off
    val invalidPartitionStrings =
      Table(
        "partitionString",
        "invalid partition string",
        "invalid partition string=42",
        //"/",
        "")
    // format: on

    forAll(invalidPartitionStrings) { partitionStr =>
      an[Exception] should be thrownBy SparkHiveMetastore.parsePartition(partitionStr)
    }
  }

  "Rendering a partition" should "produce a valid Hive partition expression" in {

    val testData = Table(
      ("partition", "expected partition expression"),
      Partition(
        List(ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
             ColumnValue(PartitionColumn("processed_date"), "2019-01-31"))) -> "(event_date='2019-01-30',processed_date='2019-01-31')",
      Partition(List(ColumnValue(PartitionColumn("date"), "2019-01-31"))) -> "(date='2019-01-31')"
    )

    forAll(testData) { (partition, expectedExpr) =>
      SparkHiveMetastore.toHivePartitionExpr(partition) shouldBe expectedExpr
    }
  }

  "Parsing the version from versioned paths" should "produce the version number" in {
    parseVersion(new URI("file:/tmp/7bbc577c-471d-4ece-8462/table/date=2019-01-21/v5")) shouldBe Version(5)
    parseVersion(new URI("s3://bucket/pageview/date=2019-01-21/v5")) shouldBe Version(5)
    parseVersion(new URI("s3://bucket/identity/v42")) shouldBe Version(42)
  }

  "Parsing the version from unversioned paths" should "produce version 0" in {
    parseVersion(new URI("s3://bucket/pageview/date=2019-01-21")) shouldBe Version(0)
    parseVersion(new URI("s3://bucket/identity")) shouldBe Version(0)
  }

  "Converting a partition path to a Hive partition expression" should "do the expected conversion" in {
    val testData = Table(
      ("partition path", "expected Hive partition expression"),
      ("date=2019-01-30", "(date='2019-01-30')"),
      ("event_date=2019-01-30/processed_date=2019-01-31", "(event_date='2019-01-30',processed_date='2019-01-31')")
    )

    forAll(testData) { (partitionPath, expected) =>
      SparkHiveMetastore.toPartitionExpr(partitionPath) shouldBe expected
    }
  }

  "Getting the base path from a versioned path" should "return the same path if it's already unversioned" in {
    versionedToBasePath(new URI("hdfs://bucket/identity")) shouldBe new URI("hdfs://bucket/identity")
  }

  it should "return strip off the version part of the path" in {
    versionedToBasePath(new URI("hdfs://bucket/identity/v42")) shouldBe new URI("hdfs://bucket/identity")
  }

  private def initPartitionedTable(table: TableDefinition): IO[Unit] = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `path` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`date` date)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    IO(spark.sql(ddl)).void
  }

  private def initSnapshotTable(table: TableDefinition): IO[Unit] = {
    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `id` string,
                 |  `name` string,
                 |  `email` string
                 |)
                 |STORED AS parquet
                 |LOCATION '${table.location}'
    """.stripMargin

    IO(spark.sql(ddl)).void
  }

}
