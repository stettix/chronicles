package com.gu.tableversions.core

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import com.gu.tableversions.core.InMemoryTableVersions.TableUpdates
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions.PartitionOperation.{AddPartitionVersion, RemovePartition}
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core.util.RichRef._

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryTableVersions[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends TableVersions[F] {

  override def init(table: TableName): F[Unit] =
    allUpdates.update { prev =>
      if (prev.contains(table)) prev else prev + (table -> Nil)
    }

  override def currentVersion(table: TableName): F[TableVersion] =
    // Derive current version of a table by folding over the history of changes
    for {
      allTableUpdates <- allUpdates.get
      tableUpdates <- allTableUpdates
        .get(table)
        .fold(F.raiseError[List[TableUpdate]](new Exception(s"Table '$table' not found")))(F.pure)
      operations = tableUpdates.flatMap(_.partitionUpdates)
    } yield InMemoryTableVersions.applyUpdate(TableVersion.empty)(operations)

  override def nextVersions(table: TableName, partitions: List[Partition]): F[List[PartitionVersion]] = {
    def maxVersions(operations: List[PartitionVersion]): Map[Partition, VersionNumber] =
      operations.foldLeft(Map.empty[Partition, VersionNumber]) { (agg, partitionVersion) =>
        val previousVersion =
          agg.getOrElse(partitionVersion.partition, VersionNumber(0))
        val maxVersion =
          if (previousVersion.number > partitionVersion.version.number) previousVersion else partitionVersion.version

        agg + (partitionVersion.partition -> maxVersion)
      }

    def nextVersion(previousVersion: Option[VersionNumber]): VersionNumber =
      previousVersion
        .map(v => VersionNumber(v.number + 1))
        .getOrElse(VersionNumber(1))

    for {
      allTableUpdates <- allUpdates.get
      tableUpdates <- allTableUpdates
        .get(table)
        .fold(F.raiseError[List[TableUpdate]](new Exception(s"Table '${table.fullyQualifiedName}' not found")))(F.pure)
      addedPartitions = tableUpdates.flatMap(_.partitionUpdates).collect {
        case AddPartitionVersion(partitionVersion) => partitionVersion
      }
      maxUsedVersions = maxVersions(addedPartitions)
      nextVersions = partitions.map(p => PartitionVersion(p, nextVersion(maxUsedVersions.get(p))))
    } yield nextVersions
  }

  override def commit(table: TableName, update: TableVersions.TableUpdate): F[TableVersions.CommitResult] = {

    // Note: we're not checking invalid partition versions here as we suspect this problem will go
    // away in a later iteration of this interface.

    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      if (currentTableUpdates.contains(table)) {
        val updated = currentTableUpdates + (table -> (currentTableUpdates(table) :+ update))
        Right(updated)
      } else
        Left(new Exception(s"Unknown table '${table.fullyQualifiedName}'"))
    }

    allUpdates.modifyEither(applyUpdate).as(SuccessfulCommit)
  }

}

object InMemoryTableVersions {

  type TableUpdates = Map[TableName, List[TableUpdate]]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryTableVersions[F]] =
    Ref[F].of(Map[TableName, List[TableUpdate]]()).map(new InMemoryTableVersions[F](_))

  /**
    * Produce current table version based on history of updates.
    */
  private[core] def applyUpdate(initial: TableVersion)(operations: List[PartitionOperation]): TableVersion = {

    def applyOp(agg: Map[Partition, VersionNumber], op: PartitionOperation): Map[Partition, VersionNumber] = op match {
      case AddPartitionVersion(partitionVersion: PartitionVersion) =>
        agg + (partitionVersion.partition -> partitionVersion.version)
      case RemovePartition(partition: Partition) =>
        agg - partition
    }

    val initialVersions: Map[Partition, VersionNumber] =
      initial.partitionVersions.map(pv => pv.partition -> pv.version).toMap

    val latestVersions = operations.foldLeft(initialVersions)(applyOp)

    val latestPartitionVersions = latestVersions.toList.map {
      case (partition, version) => PartitionVersion(partition, version)
    }

    TableVersion(latestPartitionVersions)
  }

}
