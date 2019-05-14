package com.gu.tableversions.core

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.TableOperation.{AddPartitionVersion, RemovePartition}
import org.scalatest.{FlatSpec, Matchers}

class TableVersionsObjectSpec extends FlatSpec with Matchers {

  val version1 = Version.generateVersion[IO].unsafeRunSync()
  val version2 = Version.generateVersion[IO].unsafeRunSync()
  val version3 = Version.generateVersion[IO].unsafeRunSync()

  val date = PartitionColumn("date")
  val emptyPartitionedTable = PartitionedTableVersion(Map.empty)
  "Combining partition operations" should "produce an empty table version when no updates have been applied" in {
    TableVersions.applyPartitionUpdates(emptyPartitionedTable)(Nil) shouldBe emptyPartitionedTable
  }

  it should "produce the same table when an empty update is applied" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> version1,
      Partition(date, "2019-03-02") -> version2
    )
    val tableVersion = PartitionedTableVersion(partitionVersions)
    TableVersions.applyPartitionUpdates(tableVersion)(Nil) shouldBe tableVersion
  }

  it should "produce a version with the given partitions when no previous partition versions exist" in {
    val partitionVersions = Map(
      Partition(date, "2019-03-01") -> version2,
      Partition(date, "2019-03-02") -> version1
    )
    val partitionUpdates = partitionVersions.map(AddPartitionVersion.tupled).toList
    TableVersions.applyPartitionUpdates(emptyPartitionedTable)(partitionUpdates) shouldBe PartitionedTableVersion(
      partitionVersions)
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

    TableVersions.applyPartitionUpdates(initialTableVersion)(partitionUpdates) shouldBe PartitionedTableVersion(
      expectedPartitionVersions)
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

    TableVersions.applyPartitionUpdates(initialTableVersion)(partitionUpdates) shouldBe PartitionedTableVersion(
      expectedPartitionVersions)
  }

}
