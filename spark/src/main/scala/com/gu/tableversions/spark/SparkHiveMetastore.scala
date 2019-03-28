package com.gu.tableversions.spark

import java.net.URI

import cats.effect.Sync
import cats.implicits._
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableOperation
import com.gu.tableversions.metastore.Metastore.TableOperation._
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

/**
  * Concrete implementation of the Metastore API, using Spark and Hive APIs.
  */
class SparkHiveMetastore[F[_]](implicit spark: SparkSession, F: Sync[F]) extends Metastore[F] with LazyLogging {

  import SparkHiveMetastore._
  import spark.implicits._

  override def currentVersion(table: TableName): F[TableVersion] = {

    val partitionedTableVersion: F[TableVersion] = for {
      partitions <- listPartitions(table)
      partitionLocations <- partitions.traverse { partition =>
        partitionLocation(table, toPartitionExpr(partition)).map(location => partition -> location)
      }

      partitionVersions: List[PartitionVersion] = partitionLocations.map {
        case (partition, location) => PartitionVersion(parsePartition(partition), parseVersion(location))
      }
    } yield TableVersion(partitionVersions)

    val snapshotTableVersion: F[TableVersion] = for {
      tableLocation <- findTableLocation(table)
      versionNumber = parseVersion(tableLocation)
    } yield TableVersion(List(PartitionVersion(Partition.snapshotPartition, versionNumber)))

    // Choose the calculation to perform based on whether we have a partitioned table or not
    isPartitioned(table).flatMap(partitioned => if (partitioned) partitionedTableVersion else snapshotTableVersion)
  }

  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] =
    changes.operations.traverse_(appliedOp(table))

  private def appliedOp(table: TableName)(operation: TableOperation): F[Unit] =
    operation match {
      case AddPartition(partitionVersion)           => addPartition(table, partitionVersion)
      case UpdatePartitionVersion(partitionVersion) => updatePartitionVersion(table, partitionVersion)
      case RemovePartition(partition)               => removePartition(table, partition)
      case UpdateTableVersion(versionNumber)        => updateTableLocation(table, versionNumber)
    }

  private def addPartition(table: TableName, partitionVersion: PartitionVersion): F[Unit] = {

    def addPartition(partitionExpr: String, partitionLocation: URI): F[Unit] = {
      val addPartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} ADD IF NOT EXISTS PARTITION $partitionExpr LOCATION '$partitionLocation'"
      performUpdate(s"Adding partition to table ${table.fullyQualifiedName}", addPartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partitionVersion.partition)

    versionedPartitionLocation(table, partitionVersion).flatMap(location => addPartition(partitionExpr, location).void)
  }

  private def updatePartitionVersion(table: TableName, partitionVersion: PartitionVersion): F[Unit] = {

    def updatePartition(partitionExpr: String, partitionLocation: URI): F[Unit] = {
      val updatePartitionQuery =
        s"ALTER TABLE ${table.fullyQualifiedName} PARTITION $partitionExpr SET LOCATION '$partitionLocation'"
      performUpdate(s"Updating partition version of partition ${partitionVersion.partition}", updatePartitionQuery)
    }

    val partitionExpr = toHivePartitionExpr(partitionVersion.partition)

    versionedPartitionLocation(table, partitionVersion).flatMap(location =>
      updatePartition(partitionExpr, location).void)
  }

  private def removePartition(table: TableName, partition: Partition): F[Unit] = {
    val partitionExpr = toHivePartitionExpr(partition)
    val removePartitionQuery =
      s"ALTER TABLE ${table.fullyQualifiedName} DROP IF EXISTS PARTITION $partitionExpr"
    performUpdate(s"Removing partition $partition from table ${table.fullyQualifiedName}", removePartitionQuery)
  }

  private def updateTableLocation(table: TableName, version: VersionNumber): F[Unit] = {
    def updateLocation(tableLocation: URI): F[Unit] = {
      val versionedPath = VersionPaths.pathFor(tableLocation, version)
      val updateQuery = s"ALTER TABLE ${table.fullyQualifiedName} SET LOCATION '$versionedPath'"
      performUpdate(s"Updating table location of table ${table.fullyQualifiedName}", updateQuery)
    }

    findTableLocation(table).flatMap(updateLocation).void
  }

  private def versionedPartitionLocation(table: TableName, partitionVersion: PartitionVersion): F[URI] =
    for {
      tableLocation <- findTableLocation(table)
      partitionLocation <- F.delay(partitionVersion.partition.resolvePath(tableLocation))
      versionedPartitionLocation <- F.delay(VersionPaths.pathFor(partitionLocation, partitionVersion.version))
    } yield versionedPartitionLocation

  private def performUpdate(description: String, query: String): F[Unit] =
    F.delay {
      logger.info(s"$description using query: $query")
      spark.sql(query)
    }.void

  private def findTableLocation(table: TableName): F[URI] = {
    F.delay {
      spark
        .sql(s"DESCRIBE FORMATTED ${table.fullyQualifiedName}")
        .where('col_name === "Location")
        .collect()
        .map(_.getString(1))
        .headOption
        .map(new URI(_))
        .getOrElse(throw new Exception(s"No location information returned for table ${table.fullyQualifiedName}"))
    }

  }

  private def listPartitions(table: TableName): F[List[String]] =
    F.delay { spark.sql(s"show partitions ${table.fullyQualifiedName}").collect().toList.map(_.getString(0)) }

  private def partitionLocation(table: TableName, partitionExpr: String): F[URI] =
    F.delay {
      spark
        .sql(s"DESCRIBE FORMATTED ${table.fullyQualifiedName} PARTITION $partitionExpr")
        .where('col_name === "Location")
        .collect()
        .map(_.getAs[String]("data_type"))
        .headOption
        .map(new URI(_))
        .getOrElse(throw new Exception(
          s"No location information returned for partition $partitionExpr on table ${table.fullyQualifiedName}"))
    }

  private def isPartitioned(table: TableName): F[Boolean] =
    // We have to interpret the strange format returned by DESCRIBE queries, which, if the table is partitioned,
    // contains rows like:
    //
    // |# Partition Information|         |               |
    // |# col_name             |data_type|comment        |
    // |date                   |date     |null           |
    F.delay {
      spark
        .sql(s"DESCRIBE ${table.fullyQualifiedName}")
        .collect()
        .flatMap(row => Option(row.getAs[String]("col_name")))
        .contains("# Partition Information")
    }

}

object SparkHiveMetastore {

  private[spark] def toPartitionExpr(partitionPath: String): String =
    toHivePartitionExpr(parsePartition(partitionPath))

  private[spark] def toHivePartitionExpr(partition: Partition): String =
    partition.columnValues
      .map(columnValue => s"${columnValue.column.name}='${columnValue.value}'")
      .mkString("(", ",", ")")

  private val ColumnValueRegex = """(?x)
                                   |([a-z_]+)  # column name
                                   |=
                                   |(.+)       # column value
                                 """.stripMargin.r

  private[spark] def parsePartition(partitionStr: String): Partition = {
    def parseColumnValue(str: String): ColumnValue = str match {
      case ColumnValueRegex(columnName, value) => ColumnValue(PartitionColumn(columnName), value)
      case _                                   => throw new Exception(s"Invalid partition string: $partitionStr")
    }
    val parts = partitionStr.split("/").toList
    val columnValues = parts.map(parseColumnValue)
    Partition(columnValues)
  }

  private val VersionRegex = "v(\\d+)".r

  private[spark] def parseVersion(location: URI): VersionNumber = {
    val maybeVersionStr = location.toString.split("/").lastOption
    maybeVersionStr match {
      case Some(VersionRegex(versionStr)) => VersionNumber(versionStr.toInt)
      case _                              => VersionNumber(0)
    }
  }

}
