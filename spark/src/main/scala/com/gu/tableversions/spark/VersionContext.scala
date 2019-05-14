package com.gu.tableversions.spark

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.gu.tableversions.core.Metastore.TableChanges
import com.gu.tableversions.core.TableVersions.TableOperation._
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core._
import com.gu.tableversions.spark.filesystem.VersionedFileSystem
import com.gu.tableversions.spark.filesystem.VersionedFileSystem.VersionedFileSystemConfig
import org.apache.spark.sql.{Dataset, Row, SaveMode}

/**
  * Code for writing Spark datasets to storage in a version-aware manner, taking in version information,
  * using the appropriate paths for storage, and committing version changes.
  */
final case class VersionContext(
    metastore: VersionedMetastore[IO],
    generateVersion: IO[Version]
)

final case class SparkSupport(versionContext: VersionContext) {

  object syntax {

    implicit class DatasetOps[T](val delegate: Dataset[T]) {

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
        SparkSupport.versionedInsertDatasetIntoTable(versionContext, delegate, table, userId, message).unsafeRunSync()
    }
  }
}

object SparkSupport {

  /**
    * Keep default (public) scope. It was `private` before and failed at Runtime (yet compiled...).
    * Have not tried with other scopes.
    */
  def versionedInsertDatasetIntoTable[T](
      versionContext: VersionContext,
      dataset: Dataset[T],
      table: TableDefinition,
      userId: UserId,
      message: String): IO[(TableVersion, TableChanges)] = {

    import versionContext._

    def writePartitionedDataset(version: Version): IO[List[TableOperation]] =
      for {
        // Find the partition values in the given dataset
        datasetPartitions <- IO(partitionValues(dataset, table.partitionSchema))

        // Use the same version for each of the partitions we'll be writing
        partitionVersions = datasetPartitions.map(p => p -> version).toMap

        // Write Spark dataset to the versioned path
        _ <- IO(writeVersionedPartitions(dataset, table, partitionVersions))

      } yield datasetPartitions.map(partition => AddPartitionVersion(partition, version))

    def writeSnapshotDataset(version: Version): IO[List[TableOperation]] = {
      val path = VersionPaths.pathFor(table.location, version)
      IO(dataset.write.parquet(path.toString)).as(List(AddTableVersion(version)))
    }

    for {
      newVersion <- generateVersion

      // Write the data
      operations <- if (table.isSnapshot) writeSnapshotDataset(newVersion) else writePartitionedDataset(newVersion)

      // Commit version change and update metastore
      result <- metastore.commit(table.name, TableUpdate(userId, UpdateMessage(message), Instant.now(), operations))

    } yield result
  }

  /**
    * Get the unique partition values that exist within the given dataset, based on given partition columns.
    */
  private[spark] def partitionValues[T](dataset: Dataset[T], partitionSchema: PartitionSchema): List[Partition] = {
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
      partitionVersions: Map[Partition, Version]): Unit = {

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
