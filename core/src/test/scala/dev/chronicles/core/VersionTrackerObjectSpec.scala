package dev.chronicles.core

import cats.effect.IO
import dev.chronicles.core.Partition.PartitionColumn
import dev.chronicles.core.VersionTracker.TableOperation.{AddPartitionVersion, RemovePartition}
import org.scalatest.{FlatSpec, Matchers}
import fs2.Stream

class VersionTrackerObjectSpec extends FlatSpec with Matchers {

  val version1 = Version.generateVersion[IO].unsafeRunSync()
  val version2 = Version.generateVersion[IO].unsafeRunSync()
  val version3 = Version.generateVersion[IO].unsafeRunSync()

  val date = PartitionColumn("date")
  val emptyPartitionedTable = PartitionedTableVersion(Map.empty)

  "Combining partition operations" should "produce an empty table version when no updates have been applied" in {
    VersionTracker
      .applyPartitionUpdates(emptyPartitionedTable)(Stream.empty)
      .compile
      .toList shouldBe List(emptyPartitionedTable)
  }

  it should "produce the same table when an empty update is applied" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> version1,
      Partition(date, "2019-03-02") -> version2
    )
    val tableVersion = PartitionedTableVersion(partitionVersions)
    VersionTracker.applyPartitionUpdates(tableVersion)(Stream.empty).compile.toList shouldBe List(tableVersion)
  }

  it should "produce a version with the given partitions when no previous partition versions exist" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> version2,
      Partition(date, "2019-03-02") -> version1
    )
    val partitionUpdates = partitionVersions.map(AddPartitionVersion.tupled).toList
    VersionTracker
      .applyPartitionUpdates(emptyPartitionedTable)(Stream.emits(partitionUpdates))
      .compile
      .toList shouldBe List(PartitionedTableVersion(partitionVersions))
  }

  it should "pick the latest version when an existing partition version is updated" in {

    val initialPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version1,
      Partition(date, "2019-03-02") -> version2,
      Partition(date, "2019-03-03") -> version1
    )
    val initialTableVersion = PartitionedTableVersion(initialPartitionVersions)

    val partitionUpdates = List(AddPartitionVersion(Partition(date, "2019-03-02"), version3))

    val expectedPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version1,
      Partition(date, "2019-03-02") -> version3,
      Partition(date, "2019-03-03") -> version1
    )

    VersionTracker
      .applyPartitionUpdates(initialTableVersion)(Stream.emits(partitionUpdates))
      .compile
      .toList shouldBe List(PartitionedTableVersion(expectedPartitionVersions))
  }

  it should "remove an existing partition" in {
    val initialPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version3,
      Partition(date, "2019-03-02") -> version2,
      Partition(date, "2019-03-03") -> version1
    )
    val initialTableVersion = PartitionedTableVersion(initialPartitionVersions)

    val partitionUpdates = List(RemovePartition(Partition(date, "2019-03-02")))

    val expectedPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version3,
      Partition(date, "2019-03-03") -> version1
    )

    VersionTracker
      .applyPartitionUpdates(initialTableVersion)(Stream.emits(partitionUpdates))
      .compile
      .toList shouldBe List(PartitionedTableVersion(expectedPartitionVersions))
  }

}
