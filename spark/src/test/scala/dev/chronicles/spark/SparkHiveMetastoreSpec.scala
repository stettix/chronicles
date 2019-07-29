package dev.chronicles.spark

import cats.effect.IO
import cats.syntax.functor._
import dev.chronicles.core.Partition.{ColumnValue, PartitionColumn}
import dev.chronicles.core._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SparkHiveMetastoreSpec extends FlatSpec with Matchers with SparkHiveSuite with MetastoreSpec with PropertyChecks {

  val snapshotTable =
    TableDefinition(TableName(schema, "users"), resolveTablePath("users"), PartitionSchema.snapshot, FileFormat.Parquet)

  val partitionedTable = TableDefinition(TableName(schema, "clicks"),
                                         resolveTablePath("clicks"),
                                         PartitionSchema(List(PartitionColumn("date"))),
                                         FileFormat.Parquet)
  //
  // Common specs
  //

  "A metastore implementation" should behave like metastoreWithSnapshotSupport(IO { new SparkHiveMetastore },
                                                                               initSnapshotTable(snapshotTable),
                                                                               snapshotTable.name)

  it should behave like metastoreWithPartitionsSupport(IO { new SparkHiveMetastore },
                                                       initPartitionedTable(partitionedTable),
                                                       partitionedTable.name)

  "Rendering a partition" should "produce a valid Hive partition expression" in {

    val testData = Table(
      ("partition", "expected partition expression"),
      Partition(
        ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
        ColumnValue(PartitionColumn("processed_date"), "2019-01-31")) -> "(event_date='2019-01-30',processed_date='2019-01-31')",
      Partition(ColumnValue(PartitionColumn("date"), "2019-01-31")) -> "(date='2019-01-31')"
    )

    forAll(testData) { (partition, expectedExpr) =>
      SparkHiveMetastore.toHivePartitionExpr(partition) shouldBe expectedExpr
    }
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
