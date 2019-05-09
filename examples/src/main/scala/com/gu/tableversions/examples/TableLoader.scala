package com.gu.tableversions.examples

import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core.{TableDefinition, TableVersions, Version}
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.spark.VersionedDataset
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
  * Example code for loading data into versioned tables and updating the versions of such tables.
  */
class TableLoader[T <: Product: TypeTag](table: TableDefinition, createTableDdl: String, isSnapshot: Boolean)(
    implicit tableVersions: TableVersions[IO],
    metastore: Metastore[IO],
    generateVersion: IO[Version],
    spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def initTable(userId: UserId, message: UpdateMessage): Unit = {
    // Create table in underlying metastore
    spark.sql(createTableDdl)

    // Initialise version tracking for table
    tableVersions.init(table.name, isSnapshot, userId, message, Instant.now()).unsafeRunSync()
  }

  def data(): Dataset[T] =
    spark.table(table.name.fullyQualifiedName).as[T]

  def insert(dataset: Dataset[T], userId: UserId, message: String): Unit = {
    import VersionedDataset._

    val (latestVersion, metastoreChanges) = dataset.versionedInsertInto(table, userId, message)

    logger.info(s"Updated table $table, new version details:\n$latestVersion")
    logger.info(s"Applied the the following changes to sync the Metastore:\n$metastoreChanges")
  }

  def checkout(id: TableVersions.CommitId): Unit = {
    val checkout = for {
      _ <- tableVersions.setCurrentVersion(table.name, id)
      newVersion <- tableVersions.currentVersion(table.name)
      currentMetastoreVersion <- metastore.currentVersion(table.name)
      changes = metastore.computeChanges(currentMetastoreVersion, newVersion)
      _ <- metastore.update(table.name, changes)
    } yield ()

    checkout.unsafeRunSync()
  }
}
