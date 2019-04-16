package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{Partition, Version}
import org.scalatest.{FlatSpec, Matchers}

class VersionPathsSpec extends FlatSpec with Matchers {

  "Resolving a path with version 0" should "produce the original path" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), Version(0)) shouldBe new URI("s3://foo/bar/")
  }

  it should "return a normalised path if given a path that doesn't end in a '/'" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), Version(0)) shouldBe new URI("s3://foo/bar/")
  }

  "Resolving path with a non-0 version number" should "have the version string appended as a folder" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), Version(42)) shouldBe new URI("s3://foo/bar/v42")
  }

  "Resolving versioned paths" should "return paths relative to the table location, using the defined versioning scheme" in {
    val tableLocation = new URI("s3://bucket/data/")

    val partitionVersions = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> Version(5),
      Partition(PartitionColumn("date"), "2019-01-16") -> Version(1),
      Partition(PartitionColumn("date"), "2019-01-18") -> Version(3)
    )

    val partitionPaths = VersionPaths.resolveVersionedPartitionPaths(partitionVersions, tableLocation)

    partitionPaths shouldBe Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> new URI("s3://bucket/data/date=2019-01-15/v5"),
      Partition(PartitionColumn("date"), "2019-01-16") -> new URI("s3://bucket/data/date=2019-01-16/v1"),
      Partition(PartitionColumn("date"), "2019-01-18") -> new URI("s3://bucket/data/date=2019-01-18/v3")
    )
  }

  it should "correctly resolved paths even if the base bath doesn't have a trailing slash" in {
    val tableLocation = new URI("s3://bucket/data")

    val partitionVersions = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> Version(5)
    )

    val partitionPaths = VersionPaths.resolveVersionedPartitionPaths(partitionVersions, tableLocation)

    partitionPaths shouldBe Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> new URI("s3://bucket/data/date=2019-01-15/v5")
    )
  }

}
