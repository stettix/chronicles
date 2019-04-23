package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{Partition, Version}
import org.scalatest.{FlatSpec, Matchers}

class VersionPathsSpec extends FlatSpec with Matchers {

  val version = Version.generateVersion.unsafeRunSync()

  "Resolving path with a version" should "have the version string appended as a folder" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), version) shouldBe new URI(s"s3://foo/bar/${version.label}")
  }

  it should "return a normalised path if given a path that doesn't end in a '/'" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), version) shouldBe new URI(s"s3://foo/bar/${version.label}")
  }

  it should "return the original path if no actual version is given" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), Version.Unversioned) shouldBe new URI("s3://foo/bar")
  }

  "Resolving versioned paths" should "return paths relative to the table location, using the defined versioning scheme" in {
    val tableLocation = new URI("s3://bucket/data/")

    val partitions = List(
      Partition(PartitionColumn("date"), "2019-01-15"),
      Partition(PartitionColumn("date"), "2019-01-16"),
      Partition(PartitionColumn("date"), "2019-01-18")
    )

    val partitionPaths = VersionPaths.resolveVersionedPartitionPaths(partitions, version, tableLocation)

    partitionPaths shouldBe Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> new URI(s"s3://bucket/data/date=2019-01-15/${version.label}"),
      Partition(PartitionColumn("date"), "2019-01-16") -> new URI(s"s3://bucket/data/date=2019-01-16/${version.label}"),
      Partition(PartitionColumn("date"), "2019-01-18") -> new URI(s"s3://bucket/data/date=2019-01-18/${version.label}")
    )
  }

  it should "correctly resolved paths even if the base bath doesn't have a trailing slash" in {
    val tableLocation = new URI("s3://bucket/data")

    val partitions = List(Partition(PartitionColumn("date"), "2019-01-15"))

    val partitionPaths =
      VersionPaths.resolveVersionedPartitionPaths(partitions, version, tableLocation)

    partitionPaths shouldBe Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> new URI(s"s3://bucket/data/date=2019-01-15/${version.label}")
    )
  }

}
