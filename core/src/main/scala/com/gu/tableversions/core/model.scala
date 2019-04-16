package com.gu.tableversions.core

import java.net.URI

/**
  * A Partition represents a concrete partition of a table, i.e. a partition column with a specific value.
  */
final case class Partition(columnValues: List[Partition.ColumnValue]) {

  /** Given a base path for the table, return the path to the partition. */
  def resolvePath(tableLocation: URI): URI = {
    def normalised(dir: URI): URI = if (dir.toString.endsWith("/")) dir else new URI(dir.toString + "/")
    if (this == Partition.snapshotPartition) {
      tableLocation
    } else {
      val partitionsSuffix =
        columnValues.map(columnValue => s"${columnValue.column.name}=${columnValue.value}").mkString("", "/", "/")
      normalised(tableLocation).resolve(partitionsSuffix)
    }
  }
}

object Partition {

  /** Convenience constructor for single column partitions. */
  def apply(columnValue: ColumnValue): Partition = Partition(List(columnValue))

  /** Convenience constructor for single column partitions. */
  def apply(column: PartitionColumn, value: String): Partition = Partition(List(ColumnValue(column, value)))

  case class PartitionColumn(name: String) extends AnyVal

  case class ColumnValue(column: PartitionColumn, value: String)

  // The special case partition that represents the root partition of a snapshot table.
  val snapshotPartition: Partition = Partition(Nil)

}

/**
  * A partition schema describes the fields used for partitions of a table
  */
final case class PartitionSchema(columns: List[Partition.PartitionColumn])

object PartitionSchema {

  // The special case partition that represents the root partition of a snapshot table.
  val snapshot: PartitionSchema = PartitionSchema(Nil)

}

//
// Versions
//

final case class Version(number: Int) extends AnyVal

//
// Tables
//

final case class TableName(schema: String, name: String) {
  def fullyQualifiedName: String = s"$schema.$name"
}

final case class TableDefinition(name: TableName, location: URI, partitionSchema: PartitionSchema)

/**
  * The complete set of version information for all partitions in a table.
  */
final case class TableVersion(partitionVersions: Map[Partition, Version])

object TableVersion {

  def snapshotVersion(version: Version): TableVersion = TableVersion(Map(Partition.snapshotPartition -> version))

  val empty = TableVersion(Map.empty)

}
