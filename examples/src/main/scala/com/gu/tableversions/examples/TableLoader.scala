package com.gu.tableversions.examples

import java.time.Instant

import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core.{TableDefinition, TableVersions}
import com.gu.tableversions.spark.{SparkSupport, VersionContext}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
  * Example code for loading data into versioned tables and updating the versions of such tables.
  */
class TableLoader[T <: Product: TypeTag](
    versionContext: VersionContext,
    table: TableDefinition,
    createTableDdl: String,
    isSnapshot: Boolean)(implicit spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._
  import versionContext._
  val ss = SparkSupport(versionContext)
  import ss.syntax._

  def initTable(userId: UserId, message: UpdateMessage): Unit = {
    // Create table in underlying metastore
    spark.sql(createTableDdl)

    // Initialise version tracking for table
    tableVersions.init(table.name, isSnapshot, userId, message, Instant.now()).unsafeRunSync()
  }

  def data(): Dataset[T] =
    spark.table(table.name.fullyQualifiedName).as[T]

  def insert(dataset: Dataset[T], userId: UserId, message: String): Unit = {
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
