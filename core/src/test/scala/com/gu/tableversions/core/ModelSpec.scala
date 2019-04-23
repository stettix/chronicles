package com.gu.tableversions.core

import java.net.URI

import cats.implicits._
import com.gu.tableversions.core.Partition.PartitionColumn
import org.scalatest.{FlatSpec, Matchers}

class ModelSpec extends FlatSpec with Matchers {

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

  "Generating versions" should "produce valid labels" in {
    val version = Version.generateVersion.unsafeRunSync()
    version.label should fullyMatch regex Version.TimestampAndUuidRegex
  }

  it should "produce unique labels" in {
    val versions = (1 to 100).map(_ => Version.generateVersion).toList.sequence.unsafeRunSync()
    versions.distinct.size shouldBe versions.size
  }

  "The label format regex" should "match valid labels" in {
    val validVersionLabel = "20181102-235900-4920d06f-2233-4b4a-9521-8e730eee89c5"
    validVersionLabel should fullyMatch regex Version.TimestampAndUuidRegex
  }

}
