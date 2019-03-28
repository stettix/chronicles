package com.gu.tableversions.metastore

import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.Metastore.TableOperation._

/**
  * Defines the API for manipulating and querying a Metastore for versioned tables.
  *
  * The operations defined here support both snapshot and partitioned tables.
  */
trait Metastore[F[_]] {

  /**
    * Describe the current table in the Metastore interpreted in terms of version information.
    *
    * @param table The table to query
    * @return the table version information, or a failure if the table doesn't exist or can't be queried.
    */
  def currentVersion(table: TableName): F[TableVersion]

  /**
    * Apply the given changes to the table in the Hive Metastore.
    *
    * @param table The table to update
    * @param changes The changes that need to be applied to the table
    * @return failure if the table is unknown or can't be updated.
    */
  def update(table: TableName, changes: TableChanges): F[Unit]

  /**
    * @return the set of changes that need to be applied to the Metastore to convert the `current` table
    *         to the `target` table.
    */
  def computeChanges(current: TableVersion, target: TableVersion): TableChanges =
    Metastore.computeChanges(current, target)

}

object Metastore {

  final case class TableChanges(operations: List[TableOperation])

  object TableChanges {
    def apply(operations: TableOperation*): TableChanges = TableChanges(operations.toList)
  }

  sealed trait TableOperation

  object TableOperation {
    final case class AddPartition(partitionVersion: PartitionVersion) extends TableOperation
    final case class UpdatePartitionVersion(partitionVersion: PartitionVersion) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
    final case class UpdateTableVersion(versionNumber: VersionNumber) extends TableOperation
  }

  def computeChanges(oldVersion: TableVersion, newVersion: TableVersion): TableChanges = {

    val operations = if (oldVersion.partitionVersions.map(_.partition) == List(Partition.snapshotPartition)) {
      assert(newVersion.partitionVersions.map(_.partition) == List(Partition.snapshotPartition),
             "Can't change table from snapshot table to partitioned")

      if (oldVersion.partitionVersions.head.version != newVersion.partitionVersions.head.version)
        List(UpdateTableVersion(newVersion.partitionVersions.head.version))
      else
        Nil

    } else {
      val oldPartitionVersions = oldVersion.partitionVersions.map(pv => pv.partition -> pv.version).toMap
      val newPartitionVersions = newVersion.partitionVersions.map(pv => pv.partition -> pv.version).toMap

      val oldPartitions = oldVersion.partitionVersions.map(_.partition)
      val newPartitions = newVersion.partitionVersions.map(_.partition)

      val addedPartitions = newPartitions diff oldPartitions
      val removedPartitions = oldPartitions diff newPartitions
      val updatedPartitions = (oldPartitions intersect newPartitions).filter { partition =>
        oldPartitionVersions(partition) != newPartitionVersions(partition)
      }

      val addOperations =
        addedPartitions.map(partition => AddPartition(PartitionVersion(partition, newPartitionVersions(partition))))
      val removeOperations =
        removedPartitions.map(RemovePartition)
      val updateOperations = updatedPartitions.map(partition =>
        UpdatePartitionVersion(PartitionVersion(partition, newPartitionVersions(partition))))

      addOperations ++ removeOperations ++ updateOperations
    }

    TableChanges(operations)
  }

}
