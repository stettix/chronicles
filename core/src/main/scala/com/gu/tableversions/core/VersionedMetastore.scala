package com.gu.tableversions.core

import java.time.Instant

import cats.effect.Sync
import cats.implicits._
import com.gu.tableversions.core.Metastore.TableChanges
import com.gu.tableversions.core.TableVersions._

/**
  * High level API for table version tracking, that aggregates the functionality from TableVersions and Metastore
  * in order to provide its functionality.
  */
final case class VersionedMetastore[F[_]: Sync](tableVersions: TableVersions[F], metastore: Metastore[F]) {

  /**
    * Start tracking version information for given table.
    * This must be called before any other operations can be performed on this table.
    */
  def init(table: TableName, isSnapshot: Boolean, userId: UserId, message: UpdateMessage, timestamp: Instant): F[Unit] =
    tableVersions.init(table, isSnapshot, userId, message, timestamp)

  /**
    * Get details about partition versions in a table.
    */
  def currentVersion(table: TableName): F[TableVersion] =
    tableVersions.currentVersion(table)

  /**
    * Get the history of updates for a given table, most recent first.
    */
  def updates(table: TableName): F[List[TableUpdateMetadata]] =
    tableVersions.updates(table)

  /**
    * Update partition versions to the given versions, and update the metastore to match.
    *
    * @return a tuple containing the updated table version information, and a list of the changes that were applied
    *         to the metastore.
    */
  def commit(table: TableName, update: TableUpdate): F[(TableVersion, TableChanges)] =
    for {
      // Commit version to version history
      _ <- tableVersions.commit(table, update)

      // Get latest version details and Metastore table details and find the changes that
      // need to be applied to the underlying metastore
      latestTableVersion <- tableVersions.currentVersion(table)
      metastoreVersion <- metastore.currentVersion(table)
      metastoreChanges = metastore.computeChanges(metastoreVersion, latestTableVersion)

      // Sync Metastore to match
      _ <- metastore.update(table, metastoreChanges)
    } yield (latestTableVersion, metastoreChanges)

  /**
    * Select an existing version as the current one and update the metastore to match.
    */
  def checkout(table: TableName, id: CommitId): F[Unit] =
    for {
      _ <- tableVersions.setCurrentVersion(table, id)
      newVersion <- tableVersions.currentVersion(table)
      currentMetastoreVersion <- metastore.currentVersion(table)
      changes = metastore.computeChanges(currentMetastoreVersion, newVersion)
      _ <- metastore.update(table, changes)
    } yield ()

}
