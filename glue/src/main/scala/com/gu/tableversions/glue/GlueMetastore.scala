package com.gu.tableversions.glue

import java.net.URI

import cats.effect.Sync
import cats.implicits._
import com.amazonaws.services.glue.AWSGlue
import com.amazonaws.services.glue.model.{Partition => GluePartition, Table => GlueTable, TableVersion => _, _}
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.core.Metastore.TableOperation
import com.gu.tableversions.core.Metastore.TableOperation._
import com.gu.tableversions.core.{Metastore, VersionPaths}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

class GlueMetastore[F[_]](glue: AWSGlue)(implicit F: Sync[F]) extends Metastore[F] with LazyLogging {
  import GlueMetastore._
  override def currentVersion(table: TableName): F[TableVersion] = {

    def getPartitionColumns(glueTable: GlueTable): List[PartitionColumn] =
      Option(glueTable.getPartitionKeys).toList.flatten.map { glueColumn =>
        PartitionColumn(glueColumn.getName)
      }

    def snapshotTableVersion(tableLocation: URI): TableVersion =
      SnapshotTableVersion(VersionPaths.parseVersion(tableLocation))

    for {
      glueTable <- getGlueTable(table)
      partitionColumns = getPartitionColumns(glueTable)
      tableLocation = findTableLocation(glueTable)
      version <- if (partitionColumns.isEmpty)
        F.pure(snapshotTableVersion(tableLocation))
      else
        getPartitionedTableVersion(table, tableLocation, partitionColumns)
    } yield version
  }

  private def getPartitionedTableVersion(
      table: TableName,
      tableLocation: URI,
      partitionColumns: List[PartitionColumn]): F[TableVersion] = {

    def toPartition(columnValues: List[ColumnValue]): Partition = columnValues match {
      case head :: tail => Partition(head, tail: _*)
      case _            => throw new Exception("empty columnValues list for partition found")
    }

    def toPartitionWithVersion(gluePartition: GluePartition): (Partition, Version) = {

      val partitionColumnAndValue: List[(PartitionColumn, String)] = partitionColumns.zip(gluePartition.getValues)
      val columnValues: List[ColumnValue] = partitionColumnAndValue.map(ColumnValue.tupled)
      val partition: Partition = toPartition(columnValues)
      val location = new URI(gluePartition.getStorageDescriptor.getLocation)
      partition -> VersionPaths.parseVersion(location)
    }

    getGluePartitions(table).map { gluePartitions =>
      val partitionVersions: Map[Partition, Version] = gluePartitions.map(toPartitionWithVersion).toMap
      PartitionedTableVersion(partitionVersions)
    }

  }

  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] =
    changes.operations.traverse_(appliedOp(table))

  private def appliedOp(table: TableName)(operation: TableOperation): F[Unit] =
    operation match {
      case AddPartition(partition, version)           => addPartition(table, partition, version)
      case UpdatePartitionVersion(partition, version) => updatePartitionVersion(table, partition, version)
      case RemovePartition(partition)                 => removePartition(table, partition)
      case UpdateTableVersion(versionNumber)          => updateTableLocation(table, versionNumber)
    }

  private[glue] def addPartition(table: TableName, partition: Partition, version: Version): F[Unit] = {
    logger.info(s"adding partition ${partition.columnValues.toList} for table $table and $version")

    def partitionLocation(tableLocation: URI): String = {
      val unversionedLocation: String = partition.resolvePath(tableLocation).toString
      if (version == Version.Unversioned) unversionedLocation
      else
        unversionedLocation + version.label
    }
    for {
      glueTable <- getGlueTable(table)
      tableLocation = findTableLocation(glueTable)
      location = partitionLocation(tableLocation)
      partitionStorageDescriptor = extractFormatParams(glueTable.getStorageDescriptor).withLocation(location)
      partitionValues = partition.columnValues.map(_.value).toList
      input = new PartitionInput().withValues(partitionValues).withStorageDescriptor(partitionStorageDescriptor)
      addPartitionRequest = new CreatePartitionRequest()
        .withDatabaseName(table.schema)
        .withTableName(table.name)
        .withPartitionInput(input)
      _ <- F.delay { glue.createPartition(addPartitionRequest) }
    } yield ()
  }

  private[glue] def updatePartitionVersion(table: TableName, partition: Partition, version: Version): F[Unit] = {
    logger.info(s"updating partition ${partition.columnValues.toList} for table $table and $version")

    def updatePartition(storageDescriptor: StorageDescriptor): F[Unit] = {
      val partitionValues = partition.columnValues.map(_.value).toList
      val input = new PartitionInput().withValues(partitionValues).withStorageDescriptor(storageDescriptor)

      val updatePartitionRequest = new UpdatePartitionRequest()
        .withDatabaseName(table.schema)
        .withTableName(table.name)
        .withPartitionInput(input)
        .withPartitionValueList(partitionValues)

      F.delay(glue.updatePartition(updatePartitionRequest)).void
    }

    versionedPartitionStorageDescriptor(table, partition, version).flatMap(location => updatePartition(location).void)
  }

  private def versionedPartitionStorageDescriptor(
      table: TableName,
      partition: Partition,
      version: Version): F[StorageDescriptor] =
    for {
      gluetable <- getGlueTable(table)
      tableLocation = findTableLocation(gluetable)
      partitionLocation = partition.resolvePath(tableLocation)
      versionedPartitionLocation = VersionPaths.pathFor(partitionLocation, version)
    } yield extractFormatParams(gluetable.getStorageDescriptor).withLocation(versionedPartitionLocation.toString)

  def removePartition(table: TableName, partition: Partition): F[Unit] = {
    logger.info(s"removing partition ${partition.columnValues.toList} for table $table")
    val partitionValues = partition.columnValues.map(_.value).toList
    val deletePartitionRequest = new DeletePartitionRequest()
      .withDatabaseName(table.schema)
      .withTableName(table.name)
      .withPartitionValues(partitionValues)

    F.delay(glue.deletePartition(deletePartitionRequest)).void
  }

  private[glue] def updateTableLocation(table: TableName, version: Version): F[Unit] = {
    logger.info(s"updating table location for table $table")
    for {
      glueTable <- getGlueTable(table)
      glueTableLocation = new URI(glueTable.getStorageDescriptor.getLocation)
      basePath = VersionPaths.versionedToBasePath(glueTableLocation)
      versionedPath = VersionPaths.pathFor(basePath, version)
      storageDescriptor = extractFormatParams(glueTable.getStorageDescriptor).withLocation(versionedPath.toString)
      tableInput = new TableInput().withName(table.name).withStorageDescriptor(storageDescriptor)
      updateRequest = new UpdateTableRequest().withDatabaseName(table.schema).withTableInput(tableInput)
      _ <- F.delay(glue.updateTable(updateRequest))
    } yield ()
  }

  private[glue] def getGluePartitions(table: TableName): F[List[GluePartition]] = F.delay {
    val req = new GetPartitionsRequest().withDatabaseName(table.schema).withTableName(table.name)
    val getPartitionsResult: GetPartitionsResult = glue.getPartitions(req)
    getPartitionsResult.getPartitions.toList
  }

  private[glue] def getGlueTable(table: TableName): F[GlueTable] = F.delay {
    val getTableRequest = new GetTableRequest().withDatabaseName(table.schema).withName(table.name)
    val getTableResponse = glue.getTable(getTableRequest)
    getTableResponse.getTable
  }

  private[glue] def findTableLocation(glueTable: GlueTable): URI = {
    val location = glueTable.getStorageDescriptor.getLocation
    new URI(location)
  }

}

object GlueMetastore {

  def extractFormatParams(source: StorageDescriptor): StorageDescriptor = {
    val maybeSerdeInfo = Option(source.getSerdeInfo).map { serdeInfo =>
      new SerDeInfo().withSerializationLibrary(serdeInfo.getSerializationLibrary)
    }

    new StorageDescriptor()
      .withSerdeInfo(maybeSerdeInfo.orNull)
      .withInputFormat(source.getInputFormat)
      .withOutputFormat(source.getOutputFormat)
  }
}
