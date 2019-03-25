package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges

trait Metastore[F[_]] {

  /**
    * Describe the current table in the Metastore interpreted in terms of version information.
    *
    * @param table The table to query
    */
  def currentVersion(table: TableName): F[Option[TableVersion]]

  /**
    * Apply the given changes to the table in the Hive Metastore.
    *
    * @param table The table to update
    * @param changes The changes that need to be applied to the table
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

  sealed trait TableOperation

  object TableOperation {
    final case class AddPartition(partition: PartitionVersion) extends TableOperation
    final case class UpdatePartitionVersion(partition: PartitionVersion) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
    final case class UpdateTableLocation(tableLocation: URI, versionNumber: VersionNumber) extends TableOperation
  }

  def computeChanges(current: TableVersion, target: TableVersion): TableChanges = ???

}
