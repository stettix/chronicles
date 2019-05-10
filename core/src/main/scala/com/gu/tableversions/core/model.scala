package com.gu.tableversions.core

import java.net.URI

import cats.data.NonEmptyList

/**
  * A Partition represents a concrete partition of a table, i.e. a partition column with a specific value.
  */
final case class Partition(columnValues: NonEmptyList[Partition.ColumnValue]) {

  /** Given a base path for the table, return the path to the partition. */
  def resolvePath(tableLocation: URI): URI = {
    def normalised(dir: URI): URI = if (dir.toString.endsWith("/")) dir else new URI(dir.toString + "/")
    normalised(tableLocation).resolve(s"$toString/")
  }

  /**
    * Render Partition as a partition path in the format returned from SHOW PARTITIONS queries, for example:
    *
    * event_date=2019-02-09/processed_date=2019-02-09
    */
  override def toString: String =
    columnValues.map(columnValue => s"${columnValue.column.name}=${columnValue.value}").toList.mkString("", "/", "")
}

object Partition {

  /** Convenience constructor for single column partitions. */
  def apply(columnValue: ColumnValue): Partition = Partition(NonEmptyList.one(columnValue))

  /** Convenience constructor for single column partitions. */
  def apply(column: PartitionColumn, value: String): Partition = Partition(NonEmptyList.one(ColumnValue(column, value)))

  /** Convenience constructor for multiple partitions. */
  def apply(first: ColumnValue, rest: ColumnValue*): Partition = Partition(NonEmptyList(first, rest.toList))

  /**
    * Note: do not add `extends AnyVal`. It breaks kryo serialisation.
    */
  case class PartitionColumn(name: String)

  case class ColumnValue(column: PartitionColumn, value: String)

  private val ColumnValueRegex =
    """(?x)
      |([a-z][a-z0-9_]*)  # column name
      |=
      |(.+)               # column value
    """.stripMargin.r

  /**
    * Parse Partition from partition path in the format returned from SHOW PARTITIONS queries, for example:
    *
    * event_date=2019-02-09/processed_date=2019-02-09
    */
  def parse(partitionStr: String): Either[Throwable, Partition] = {
    import cats.implicits._

    def parseColumnValue(str: String): Either[Throwable, ColumnValue] = str match {
      case ColumnValueRegex(columnName, value) => Right(ColumnValue(PartitionColumn(columnName), value))
      case _                                   => Left(new Exception(s"Invalid partition string: $partitionStr"))
    }

    partitionStr.split("/").toList.traverse(parseColumnValue).flatMap {
      case head :: tail => Right(Partition(head, tail: _*))
      case _            => Left(new Exception(s"Empty partition string found: $partitionStr"))
    }
  }

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
// Tables
//

final case class TableName(schema: String, name: String) {
  def fullyQualifiedName: String = s"$schema.$name"
}

final case class TableDefinition(name: TableName, location: URI, partitionSchema: PartitionSchema, format: FileFormat) {
  def isSnapshot: Boolean = partitionSchema == PartitionSchema.snapshot
}

/**
  * The complete set of version information for all partitions in a table.
  */
sealed trait TableVersion
final case class PartitionedTableVersion(partitionVersions: Map[Partition, Version]) extends TableVersion
final case class SnapshotTableVersion(version: Version) extends TableVersion

case class FileFormat(name: String) extends AnyVal

object FileFormat {
  val Parquet = FileFormat("parquet")
  val Orc = FileFormat("orc")
}
