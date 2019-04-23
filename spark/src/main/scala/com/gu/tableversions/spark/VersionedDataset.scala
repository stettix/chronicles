package com.gu.tableversions.spark

import java.net.URI
import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.TableOperation.{AddPartitionVersion, AddTableVersion}
import com.gu.tableversions.core.TableVersions.{TableOperation, TableUpdate, UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import cats.implicits._

/**
  * Code for writing Spark datasets to storage in a version-aware manner, taking in version information,
  * using the appropriate paths for storage, and committing version changes.
  */
object VersionedDataset {

  implicit class DatasetOps[T](val delegate: Dataset[T])(
      implicit tableVersions: TableVersions[IO],
      metastore: Metastore[IO],
      generateVersion: IO[Version]) {

    /**
      * Insert the dataset into the given versioned table.
      *
      * This emulates the behaviour of Hive inserts in that it will overwrite any partitions present in the dataset,
      * while leaving other partitions unchanged.
      *
      * @return a tuple containing the updated table version information, and a list of the changes that were applied
      *         to the metastore.
      */
    def versionedInsertInto(table: TableDefinition, userId: UserId, message: String)(
        implicit tableVersions: TableVersions[IO],
        metastore: Metastore[IO]): (TableVersion, TableChanges) =
      versionedInsertDatasetIntoTable(delegate, table, userId, message).unsafeRunSync()

  }

  private def versionedInsertDatasetIntoTable[T](
      dataset: Dataset[T],
      table: TableDefinition,
      userId: UserId,
      message: String)(
      implicit tableVersions: TableVersions[IO],
      metastore: Metastore[IO],
      generateVersion: IO[Version]): IO[(TableVersion, TableChanges)] = {

    def writePartitionedDataset(version: Version): IO[List[TableOperation]] =
      for {
        // Find the partition values in the given dataset
        datasetPartitions <- IO(VersionedDataset.partitionValues(dataset, table.partitionSchema)(dataset.sparkSession))

        // Resolve the path that each partition should be written to, based on their version
        partitionPaths = VersionPaths.resolveVersionedPartitionPaths(datasetPartitions, version, table.location)

        // Write Spark dataset to the versioned path
        _ <- IO(VersionedDataset.writeVersionedPartitions(dataset, partitionPaths))

      } yield datasetPartitions.map(partition => AddPartitionVersion(partition, version))

    def writeSnapshotDataset(version: Version): IO[List[TableOperation]] = {
      val path = VersionPaths.pathFor(table.location, version)
      IO(dataset.write.parquet(path.toString)).as(List(AddTableVersion(version)))
    }

    for {
      // Get next version to use for all partitions
      newVersion <- generateVersion

      operations <- if (table.isSnapshot) writeSnapshotDataset(newVersion) else writePartitionedDataset(newVersion)

      // Commit written version
      _ <- tableVersions.commit(table.name, TableUpdate(userId, UpdateMessage(message), Instant.now(), operations))

      // Get latest version details and Metastore table details and sync the Metastore to match,
      // effectively switching the table to the new version.
      latestTableVersion <- tableVersions.currentVersion(table.name)

      metastoreVersion <- metastore.currentVersion(table.name)
      metastoreUpdate = metastore.computeChanges(metastoreVersion, latestTableVersion)

      // Sync Metastore to match
      _ <- metastore.update(table.name, metastoreUpdate)

    } yield (latestTableVersion, metastoreUpdate)
  }

  /**
    * Get the unique partition values that exist within the given dataset, based on given partition columns.
    */
  private[spark] def partitionValues[T](dataset: Dataset[T], partitionSchema: PartitionSchema)(
      implicit spark: SparkSession): List[Partition] = {
    // Query dataset for partitions
    // NOTE: this implementation has not been optimised yet
    val partitionColumnsList = partitionSchema.columns.map(_.name)
    val partitionsDf = dataset.selectExpr(partitionColumnsList: _*).distinct()
    val partitionRows = partitionsDf.collect().toList

    def rowToPartition(row: Row): Partition = {
      val partitionColumnValues: List[(Partition.PartitionColumn, String)] =
        partitionSchema.columns zip row.toSeq.map(_.toString)

      val columnValues = partitionColumnValues.map(Partition.ColumnValue.tupled)

      columnValues match {
        case head :: tail => Partition(head, tail: _*)
        case _            => throw new Exception("Empty list of partitions not valid for partitioned table")
      }
    }

    partitionRows.map(rowToPartition)
  }

  /**
    * Write the given partitioned dataset, storing each partition in the associated path.
    */
  private[spark] def writeVersionedPartitions[T](dataset: Dataset[T], partitionPaths: Map[Partition, URI]): Unit = {
    // This is a slow and inefficient implementation that writes each partition in sequence,
    // we can look into a more performant solution later.

    def filteredForPartition(partition: Partition): Dataset[T] =
      partition.columnValues.foldLeft(dataset) {
        case (filteredDataset, partitionColumn) =>
          filteredDataset.where(s"${partitionColumn.column.name} = '${partitionColumn.value}'")
      }

    val datasetsWithPaths: Map[Dataset[T], URI] =
      partitionPaths.map { case (partition, path) => filteredForPartition(partition) -> path }

    datasetsWithPaths.foreach {
      case (datasetForPartition, partitionPath) =>
        datasetForPartition.write
          .parquet(partitionPath.toString) // TODO: Take in format parameter. Or a dataset writer?
    }
  }

}
