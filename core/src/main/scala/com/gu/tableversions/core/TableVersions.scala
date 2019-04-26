package com.gu.tableversions.core

import java.time.Instant
import java.util.UUID

import cats.effect.Sync
import cats.implicits._
import com.gu.tableversions.core.TableVersions.TableOperation._
import com.gu.tableversions.core.TableVersions._

/**
  * This defines the interface for querying and updating table version information tracked by the system.
  */
trait TableVersions[F[_]] {

  /**
    * Start tracking version information for given table.
    * This must be called before any other operations can be performed on this table.
    */
  def init(table: TableName, isSnapshot: Boolean, userId: UserId, message: UpdateMessage, timestamp: Instant): F[Unit] =
    handleInit(table) {
      val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(InitTable(table, isSnapshot)))
      TableState(currentVersion = initialUpdate.metadata.id, updates = List(initialUpdate))
    }

  /**
    * Get details about partition versions in a table.
    */
  def currentVersion(table: TableName)(implicit F: Sync[F]): F[TableVersion] =
    // Derive current version of a table by folding over the history of changes
    // until either the latest or version marked as 'current' is reached.
    for {
      ts <- tableState(table)
      matchingUpdates = ts.updates.span(_.metadata.id != ts.currentVersion)
      updatesForCurrentVersion = matchingUpdates._1 ++ matchingUpdates._2.take(1)
      operations = updatesForCurrentVersion.flatMap(_.operations)
    } yield
      if (isSnapshotTable(operations))
        latestSnapshotTableVersion(operations)
      else
        applyPartitionUpdates(PartitionedTableVersion(Map.empty))(operations)

  /** Return the history of table updates, most recent first. */
  def updates(table: TableName)(implicit F: Sync[F]): F[List[TableUpdateMetadata]] =
    tableState(table).map(_.updates.map(_.metadata).reverse)

  /**
    * Update partition versions to the given versions.
    */
  def commit(table: TableName, update: TableVersions.TableUpdate): F[Unit]

  /**
    * Set the current version of a table to refer to an existing version.
    */
  def setCurrentVersion(table: TableName, id: TableVersions.CommitId): F[Unit]

  //
  // Internal operations to be provided by implementations
  //

  /**
    * Produce description of the current state of table.
    */
  protected def tableState(table: TableName): F[TableState]

  /**
    * Handle initialisation of a new table if it doesn't exist already.
    */
  protected def handleInit(table: TableName)(newTableState: => TableState): F[Unit]

}

object TableVersions {

  final case class TableUpdateMetadata(
      id: CommitId,
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant
  )

  object TableUpdateMetadata {

    def apply(userId: UserId, message: UpdateMessage, timestamp: Instant): TableUpdateMetadata =
      TableUpdateMetadata(createId(), userId, message, timestamp)

    private def createId(): CommitId = CommitId(UUID.randomUUID().toString)
  }

  final case class CommitId(id: String) extends AnyVal

  final case class UserId(value: String) extends AnyVal

  final case class UpdateMessage(content: String) extends AnyVal

  /** A collection of updates to partitions to be applied and tracked as a single atomic change. */
  final case class TableUpdate(metadata: TableUpdateMetadata, operations: List[TableOperation])

  object TableUpdate {

    def apply(
        userId: UserId,
        message: UpdateMessage,
        timestamp: Instant,
        operations: List[TableOperation]): TableUpdate =
      TableUpdate(TableUpdateMetadata(userId, message, timestamp), operations)
  }

  final case class ErrorMessage(value: String) extends AnyVal

  /** ADT for operations on tables. */
  sealed trait TableOperation

  object TableOperation {
    final case class InitTable(tableName: TableName, isSnapshot: Boolean) extends TableOperation
    final case class AddTableVersion(version: Version) extends TableOperation
    final case class AddPartitionVersion(partition: Partition, version: Version) extends TableOperation
    final case class RemovePartition(partition: Partition) extends TableOperation
  }

  /**
    * The current state of the table, including all updates and the current version, as produced by a
    * concrete implementation.
    *
    * @param currentVersion The ID of the committed update that's considered the current version.
    *                       This will refer to the latest update unless a rollback operation has been performed.
    * @param updates The list of updates that has been performed on the table.
    *                These must be returned in the order they were executed.
    */
  final case class TableState(currentVersion: CommitId, updates: List[TableUpdate])

  /**
    * Produce current partitioned table version based on history of partition updates.
    */
  def applyPartitionUpdates(initial: PartitionedTableVersion)(
      operations: List[TableOperation]): PartitionedTableVersion = {

    def applyOp(agg: Map[Partition, Version], op: TableOperation): Map[Partition, Version] = op match {
      case AddPartitionVersion(partition: Partition, version: Version) =>
        agg + (partition -> version)
      case RemovePartition(partition: Partition) =>
        agg - partition
      case _: InitTable | _: AddTableVersion => agg
    }

    val latestVersions = operations.foldLeft(initial.partitionVersions)(applyOp)

    PartitionedTableVersion(latestVersions)
  }

  /**
    * Produce current snapshot table version based on history of table version updates.
    */
  def latestSnapshotTableVersion(operations: List[TableOperation]): SnapshotTableVersion = {
    val versions = operations.collect {
      case AddTableVersion(version) => version
    }
    SnapshotTableVersion(versions.lastOption.getOrElse(Version.Unversioned))
  }

  def isSnapshotTable(operations: List[TableOperation]): Boolean = operations match {
    case InitTable(_, isSnapshot) :: _ => isSnapshot
    case _                             => throw new IllegalArgumentException("First operation should be initialising the table")
  }

  // Errors

  def unknownTableError(table: TableName): Exception = new Exception(s"Unknown table '${table.fullyQualifiedName}'")

  def unknownCommitId(id: CommitId): Exception = new Exception(s"Unknown commit ID '$id'")

}
