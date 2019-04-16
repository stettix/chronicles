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

  override def nextVersions(table: TableName, partitions: List[Partition]): F[Map[Partition, Version]] = {
    def maxVersions(operations: List[(Partition, Version)]): Map[Partition, Version] =
      operations.foldLeft(Map.empty[Partition, Version]) {
        case (agg, (partition, partitionVersion)) =>
          val previousVersion =
            agg.getOrElse(partition, Version(0))
          val maxVersion =
            if (previousVersion.number > partitionVersion.number) previousVersion else partitionVersion

          agg + (partition -> maxVersion)
      }

    def nextVersion(previousVersion: Option[Version]): Version =
      previousVersion
        .map(v => Version(v.number + 1))
        .getOrElse(Version(1))

    for {
      allTableUpdates <- allUpdates.get
      tableUpdates <- allTableUpdates
        .get(table)
        .fold(F.raiseError[List[TableUpdate]](new Exception(s"Table '${table.fullyQualifiedName}' not found")))(F.pure)
      addedPartitions = tableUpdates.flatMap(_.partitionUpdates).collect {
        case AddPartitionVersion(partition, version) => (partition, version)
      }
      maxUsedVersions = maxVersions(addedPartitions)
      nextVersions = partitions.map(p => p -> nextVersion(maxUsedVersions.get(p)))
    } yield nextVersions.toMap
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

    def applyOp(agg: Map[Partition, Version], op: PartitionOperation): Map[Partition, Version] = op match {
      case AddPartitionVersion(partition: Partition, version: Version) =>
        agg + (partition -> version)
      case RemovePartition(partition: Partition) =>
        agg - partition
    }

    val latestVersions = operations.foldLeft(initial.partitionVersions)(applyOp)

    TableVersion(latestVersions)
  }

}
