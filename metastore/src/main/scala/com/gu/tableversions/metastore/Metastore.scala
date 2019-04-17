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
    final case class AddPartition(partition: Partition, version: Version) extends TableOperation
    final case class UpdatePartitionVersion(partition: Partition, version: Version) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
    final case class UpdateTableVersion(versionNumber: Version) extends TableOperation
  }

  def computeChanges(oldVersion: TableVersion, newVersion: TableVersion): TableChanges =
    (oldVersion, newVersion) match {
      case (SnapshotTableVersion(oldSnapshotVersion), SnapshotTableVersion(newSnapshotVersion)) =>
        if (oldSnapshotVersion != newSnapshotVersion) TableChanges(List(UpdateTableVersion(newSnapshotVersion)))
        else TableChanges(Nil)

      case (PartitionedTableVersion(oldPartitionVersions), PartitionedTableVersion(newPartitionVersions)) => {
        val oldPartitions = oldPartitionVersions.keys.toList
        val newPartitions = newPartitionVersions.keys.toList

        val addedPartitions = newPartitions diff oldPartitions
        val removedPartitions = oldPartitions diff newPartitions
        val updatedPartitions = (oldPartitions intersect newPartitions).filter { partition =>
          oldPartitionVersions(partition) != newPartitionVersions(partition)
        }

        val addOperations =
          addedPartitions.map(partition => AddPartition(partition, newPartitionVersions(partition)))
        val removeOperations =
          removedPartitions.map(RemovePartition)
        val updateOperations =
          updatedPartitions.map(partition => UpdatePartitionVersion(partition, newPartitionVersions(partition)))

        TableChanges((addOperations ++ removeOperations ++ updateOperations))
      }

      case _ => throw new IllegalArgumentException("Can't change table from snapshot table to partitioned")

    }

}
