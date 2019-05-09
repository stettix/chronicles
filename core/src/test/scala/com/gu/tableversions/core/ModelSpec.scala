package com.gu.tableversions.core

import java.net.URI

import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class ModelSpec extends FlatSpec with Matchers with EitherValues with TableDrivenPropertyChecks {

  val tableLocation = new URI("s3://bucket/data/")

  "Resolving the path of a partition with a single partition column" should "produce path relative to the table location" in {
    val partition = Partition(Partition.ColumnValue(PartitionColumn("date"), "2019-01-20"))
    partition.resolvePath(tableLocation) shouldBe new URI("s3://bucket/data/date=2019-01-20/")
  }

  it should "work even if the table location doesn't have a trailing slash" in {
    val tableLocation = new URI("s3://bucket/data")
    val partition = Partition(Partition.ColumnValue(PartitionColumn("date"), "2019-01-20"))
    partition.resolvePath(tableLocation) shouldBe new URI("s3://bucket/data/date=2019-01-20/")
  }

  "Resolving the path of a partition with multiple partition columns" should "produce path relative to the table location" in {
    val partition = Partition(Partition.ColumnValue(PartitionColumn("event_date"), "2019-01-20"),
                              Partition.ColumnValue(PartitionColumn("processed_date"), "2019-01-21"))

    partition.resolvePath(tableLocation) shouldBe new URI(
      "s3://bucket/data/event_date=2019-01-20/processed_date=2019-01-21/")
  }

  "Parsing a valid partition string" should "produce the expected values" in {
    val testData = Table(
      ("partitionStr", "expected"),
      ("date=2019-01-31", Partition(ColumnValue(PartitionColumn("date"), "2019-01-31"))),
      ("event_date=2019-01-30/processed_date=2019-01-31",
       Partition(ColumnValue(PartitionColumn("event_date"), "2019-01-30"),
                 ColumnValue(PartitionColumn("processed_date"), "2019-01-31"))),
      ("year=2019/month=01/day=31",
       Partition(ColumnValue(PartitionColumn("year"), "2019"),
                 ColumnValue(PartitionColumn("month"), "01"),
                 ColumnValue(PartitionColumn("day"), "31"))),
      ("date_2=2019-01-31", Partition(ColumnValue(PartitionColumn("date_2"), "2019-01-31")))
    )

    forAll(testData) { (partitionStr: String, expected: Partition) =>
      Partition.parse(partitionStr).right.value shouldBe expected
    }
  }

  "Parsing an invalid partition string" should "throw an exception" in {
    // format: off
    val invalidPartitionStrings =
      Table(
        "partitionString",
        "invalid partition string",
        "invalid partition string=42",
        "/",
        "")
    // format: on

    forAll(invalidPartitionStrings) { partitionStr =>
      Partition.parse(partitionStr) shouldBe 'left
    }
  }

}
