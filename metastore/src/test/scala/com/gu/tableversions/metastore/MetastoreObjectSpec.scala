package com.gu.tableversions.metastore

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{Partition, PartitionedTableVersion, SnapshotTableVersion, TableVersion, Version}
import com.gu.tableversions.metastore.Metastore.TableOperation._
import org.scalatest.{FlatSpec, Matchers}

class MetastoreObjectSpec extends FlatSpec with Matchers {
  val version1 = Version.generateVersion.unsafeRunSync()
  val version2 = Version.generateVersion.unsafeRunSync()
  val version3 = Version.generateVersion.unsafeRunSync()

  val date = PartitionColumn("date")

  "Computing differences" should "produce operations to add new partitions" in {
    val oldVersion = PartitionedTableVersion(Map.empty)

    val newPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version3,
      Partition(date, "2019-03-03") -> version1
    )
    val newVersion = PartitionedTableVersion(newPartitionVersions)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      AddPartition(Partition(date, "2019-03-01"), version3),
      AddPartition(Partition(date, "2019-03-03"), version1)
    )
  }

  it should "produce operations to remove deleted partitions" in {
    val oldPartitionVersions = Map(
      Partition(date, "2019-03-01") -> version3,
      Partition(date, "2019-03-03") -> version1
    )
    val oldVersion = PartitionedTableVersion(oldPartitionVersions)

    val newVersion = PartitionedTableVersion(Map.empty)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      RemovePartition(Partition(date, "2019-03-01")),
      RemovePartition(Partition(date, "2019-03-03"))
    )
  }

  it should "produce operations to update the versions of existing partitions" in {
    val oldPartitionVersions = Map(Partition(date, "2019-03-01") -> version1)
    val oldVersion = PartitionedTableVersion(oldPartitionVersions)

    val newPartitionVersions = Map(Partition(date, "2019-03-01") -> version2)
    val newVersion = PartitionedTableVersion(newPartitionVersions)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      UpdatePartitionVersion(Partition(date, "2019-03-01"), version2))
  }

  it should "produce an operation to update the version of a table for an updated snapshot table version" in {
    val oldVersion = SnapshotTableVersion(version1)
    val newVersion = SnapshotTableVersion(version2)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(UpdateTableVersion(version2))
  }

  it should "produce no change for a snapshot table with the same version" in {
    val version = SnapshotTableVersion(version1)

    val changes = Metastore.computeChanges(version, version)

    changes.operations shouldBe empty
  }

}
