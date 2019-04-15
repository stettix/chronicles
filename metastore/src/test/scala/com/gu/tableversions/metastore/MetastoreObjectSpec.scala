package com.gu.tableversions.metastore

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{Partition, PartitionVersion, TableVersion, VersionNumber}
import com.gu.tableversions.metastore.Metastore.TableOperation._
import org.scalatest.{FlatSpec, Matchers}

class MetastoreObjectSpec extends FlatSpec with Matchers {

  val date = PartitionColumn("date")

  "Computing differences" should "produce operations to add new partitions" in {
    val oldVersion = TableVersion.empty

    val newPartitionVersions = List(
      PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(3)),
      PartitionVersion(Partition(date, "2019-03-03"), VersionNumber(1))
    )
    val newVersion = TableVersion(newPartitionVersions)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      AddPartition(PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(3))),
      AddPartition(PartitionVersion(Partition(date, "2019-03-03"), VersionNumber(1)))
    )
  }

  it should "produce operations to remove deleted partitions" in {
    val oldPartitionVersions = List(
      PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(3)),
      PartitionVersion(Partition(date, "2019-03-03"), VersionNumber(1))
    )
    val oldVersion = TableVersion(oldPartitionVersions)

    val newVersion = TableVersion.empty

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      RemovePartition(Partition(date, "2019-03-01")),
      RemovePartition(Partition(date, "2019-03-03"))
    )
  }

  it should "produce operations to update the versions of existing partitions" in {
    val oldPartitionVersions = List(PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(1)))
    val oldVersion = TableVersion(oldPartitionVersions)

    val newPartitionVersions = List(PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(2)))
    val newVersion = TableVersion(newPartitionVersions)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(
      UpdatePartitionVersion(PartitionVersion(Partition(date, "2019-03-01"), VersionNumber(2))))
  }

  it should "produce an operation to update the version of a table for an updated snapshot table version" in {
    val oldPartitionVersions = List(PartitionVersion(Partition.snapshotPartition, VersionNumber(1)))
    val oldVersion = TableVersion(oldPartitionVersions)

    val newPartitionVersions = List(PartitionVersion(Partition.snapshotPartition, VersionNumber(2)))
    val newVersion = TableVersion(newPartitionVersions)

    val changes = Metastore.computeChanges(oldVersion, newVersion)

    changes.operations should contain theSameElementsAs List(UpdateTableVersion(VersionNumber(2)))
  }

  it should "produce no change for a snapshot table with the same version" in {
    val partitionVersions = List(PartitionVersion(Partition.snapshotPartition, VersionNumber(1)))
    val version = TableVersion(partitionVersions)

    val changes = Metastore.computeChanges(version, version)

    changes.operations shouldBe empty
  }

}
