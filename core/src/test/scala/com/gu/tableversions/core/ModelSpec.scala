package com.gu.tableversions.core

import java.net.URI
import java.time.LocalDateTime
import java.util.UUID

import com.gu.tableversions.core.Partition.PartitionColumn
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import cats.implicits._

class ModelSpec extends FlatSpec with Matchers with EitherValues {

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

}
