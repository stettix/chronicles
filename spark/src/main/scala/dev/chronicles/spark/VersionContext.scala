package dev.chronicles.spark

import java.time.Instant

import cats.effect.{Effect, Sync}
import cats.implicits._
import dev.chronicles.core.Metastore.TableChanges
import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SaveMode}

/**
  * Code for writing Spark datasets to storage in a version-aware manner, taking in version information,
  * using the appropriate paths for storage, and committing version changes.
  */
final case class VersionContext[F[_]](
    metastore: VersionedMetastore[F],
    generateVersion: F[Version]
)

final case class SparkSupport[F[_]](versionContext: VersionContext[F])(implicit F: Effect[F]) {

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
        F.toIO(SparkSupport.versionedInsertDatasetIntoTable(versionContext, delegate, table, userId, message))
          .unsafeRunSync()
    }
  }
}

object SparkSupport {

  /**
    * Keep default (public) scope. It was `private` before and failed at Runtime (yet compiled...).
    * Have not tried with other scopes.
    */
  def versionedInsertDatasetIntoTable[T, F[_]](
      versionContext: VersionContext[F],
      dataset: Dataset[T],
      table: TableDefinition,
      userId: UserId,
      message: String)(implicit F: Sync[F]): F[(TableVersion, TableChanges)] = {

    import versionContext._

    // Check that the Spark config includes the dynamic overwrite partition setting, which is essential
    // for writing additional partitions to a dataset without overwriting existing data.
    val checkSparkConfigs: F[Unit] =
      if (!dataset.sparkSession.sparkContext.getConf
            .getOption("spark.sql.sources.partitionOverwriteMode")
            .contains("dynamic"))
        F.raiseError(
          new Error("The Spark configuration must have the spark.sql.sources.partitionOverwriteMode set to 'dynamic'"))
      else
        F.unit

    // Find the operations to represent the changes we're writing to the table
    def tableOperationsForVersion(version: Version): F[List[TableOperation]] =
      if (table.isSnapshot) {
        F.pure(List(AddTableVersion(version)))
      } else {
        val datasetPartitions = F.delay(partitionValues(dataset, table.partitionSchema))
        datasetPartitions.map(_.map(partition => AddPartitionVersion(partition, version)))
      }

    // Write table data to partition locations
    def writeWithVersion(version: Version): F[List[TableOperation]] =
      for {
        tableOperations <- tableOperationsForVersion(version)

        datasetWithVersion <- F.delay(dataset.withColumn("version", lit(version.label)))
        originalPartitions = table.partitionSchema.columns.map(_.name)
        partitionsWithVersion = originalPartitions :+ "version"

        _ <- F.delay(
          datasetWithVersion.toDF.write
            .partitionBy(partitionsWithVersion: _*)
            .mode(SaveMode.Append)
            .format(table.format.name)
            .save(table.location.toString))

      } yield tableOperations

    for {
      _ <- checkSparkConfigs
      newVersion <- generateVersion
      operations <- writeWithVersion(newVersion)
      result <- metastore.commit(table.name, TableUpdate(userId, UpdateMessage(message), Instant.now(), operations))
    } yield result
  }

  /**
    * Get the unique partition values that exist within the given dataset, based on given partition columns.
    */
  private[spark] def partitionValues[T](dataset: Dataset[T], partitionSchema: PartitionSchema): List[Partition] = {
    // Query dataset for partitions
    val partitionColumnsList = partitionSchema.columns.map(_.name)
    val partitionsDf = dataset.selectExpr(partitionColumnsList: _*).distinct()
    val partitionRows = partitionsDf.collect()

    def rowToPartition(row: Row): Partition = {
      val partitionColumnValues: List[(Partition.PartitionColumn, String)] =
        partitionSchema.columns zip row.toSeq.map(_.toString)

      val columnValues = partitionColumnValues.map(Partition.ColumnValue.tupled)

      columnValues match {
        case head :: tail => Partition(head, tail: _*)
        case _            => throw new Exception("Empty list of partitions not valid for partitioned table")
      }
    }

    partitionRows.map(rowToPartition).toList
  }
}
