package com.gu.tableversions.spark

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.gu.tableversions.core.TableVersions.TableOperation.{AddPartitionVersion, AddTableVersion}
import com.gu.tableversions.core.TableVersions.{TableOperation, TableUpdate, UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.{Metastore, VersionPaths}
import com.gu.tableversions.spark.filesystem.VersionedFileSystem
import com.gu.tableversions.spark.filesystem.VersionedFileSystem.VersionedFileSystemConfig
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

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
    def versionedInsertInto(table: TableDefinition, userId: UserId, message: String): (TableVersion, TableChanges) =
      versionedInsertDatasetIntoTable(delegate, table, userId, message).unsafeRunSync()

  }

  /**
    * Keep default (public) scope. It was `private` before and failed at Runtime (yet compiled...).
    * Have not tried with other scopes.
    */
  def versionedInsertDatasetIntoTable[T](dataset: Dataset[T], table: TableDefinition, userId: UserId, message: String)(
      implicit tableVersions: TableVersions[IO],
      metastore: Metastore[IO],
      generateVersion: IO[Version]): IO[(TableVersion, TableChanges)] = {

    def writePartitionedDataset(version: Version): IO[List[TableOperation]] =
      for {
        // Find the partition values in the given dataset
        datasetPartitions <- IO(VersionedDataset.partitionValues(dataset, table.partitionSchema)(dataset.sparkSession))

        // Use the same version for each of the partitions we'll be writing
        partitionVersions = datasetPartitions.map(p => p -> version).toMap

        // Write Spark dataset to the versioned path
        _ <- IO(VersionedDataset.writeVersionedPartitions(dataset, table, partitionVersions)(dataset.sparkSession))

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
  private[spark] def writeVersionedPartitions[T](
      dataset: Dataset[T],
      table: TableDefinition,
      partitionVersions: Map[Partition, Version])(implicit spark: SparkSession): Unit = {

    VersionedFileSystem.writeConfig(VersionedFileSystemConfig(partitionVersions),
                                    dataset.sparkSession.sparkContext.hadoopConfiguration)

    val partitions = table.partitionSchema.columns.map(_.name)

    dataset.toDF.write
      .partitionBy(partitions: _*)
      .mode(SaveMode.Append)
      .format(table.format.name)
      .save(VersionedFileSystem.scheme + "://" + table.location.getPath)
  }
}
