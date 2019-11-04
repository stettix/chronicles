package dev.chronicles.core

import java.time.Instant
import java.util.UUID

import cats.effect.Sync
import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import fs2.Stream

/**
  * This defines the interface for querying and updating table version information tracked by the system.
  */
trait VersionTracker[F[_]] {

  /**
    * List all known tables.
    */
  def tables(): Stream[F, TableName]

  /**
    * Start tracking version information for given table.
    * This must be called before any other operations can be performed on this table.
    */
  def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant): F[Unit]

  /**
    * Get details about partition versions in a table.
    */
  def currentVersion(table: TableName)(implicit F: Sync[F]): F[TableVersion] = {
    // Derive current version of a table by folding over the history of changes
    // until either the latest or version marked as 'current' is reached.
    val aggregateTableVersion = for {
      ts <- Stream.eval(tableState(table))
      updatesForCurrentVersion = ts.updates.takeThrough(_.metadata.id != ts.currentVersion)

      //operations = updatesForCurrentVersion.flatMap(update => Stream.emits(update.operations))
      operations <- updatesForCurrentVersion.map(update => Stream.emits(update.operations))

      snapshotTable <- Stream.eval(isSnapshotTable(table))

      tableVersion <- if (snapshotTable)
        latestSnapshotTableVersion[F](operations).covaryOutput[TableVersion]
      else
        applyPartitionUpdates(PartitionedTableVersion(Map.empty))(operations).covaryOutput[TableVersion]
    } yield tableVersion

    aggregateTableVersion.head.compile.lastOrError
  }

  /** Return the history of table updates, most recent first. */
  def updates(table: TableName)(implicit F: Sync[F]): Stream[F, TableUpdateMetadata] =
    Stream
      .eval(tableState(table))
      .flatMap(_.updates.map(_.metadata))

  /**
    * Update partition versions to the given versions.
    */
  def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit]

  /**
    * Set the current version of a table to refer to an existing version.
    */
  def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit]

  /**
    * Get the flag that indicates whether a table is a snapshot table or not.
    */
  def isSnapshotTable(table: TableName): F[Boolean]

  //
  // Internal operations to be provided by implementations
  //

  /**
    * Produce description of the current state of table.
    */
  protected def tableState(table: TableName): F[TableState[F]]

}

object VersionTracker {

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
  final case class TableState[F[_]](currentVersion: CommitId, updates: Stream[F, TableUpdate])

  /**
    * Produce current partitioned table version based on history of partition updates.
    */
  private[core] def applyPartitionUpdates[F[_]](initial: PartitionedTableVersion)(
      operations: Stream[F, TableOperation]): Stream[F, PartitionedTableVersion] = {

    def applyOp(agg: Map[Partition, Version], op: TableOperation): Map[Partition, Version] = op match {
      case AddPartitionVersion(partition: Partition, version: Version) =>
        agg + (partition -> version)
      case RemovePartition(partition: Partition) =>
        agg - partition
      case _: InitTable | _: AddTableVersion => agg
    }

    val latestVersions = operations.fold(initial.partitionVersions)(applyOp)

    latestVersions
      .lastOr(initial.partitionVersions)
      .map(partitionVersions => PartitionedTableVersion(partitionVersions))
  }

  /**
    * Produce current snapshot table version based on history of table version updates.
    */
  private def latestSnapshotTableVersion[F[_]](operations: Stream[F, TableOperation]): Stream[F, SnapshotTableVersion] =
    operations
      .collect {
        case AddTableVersion(version) => version
      }
      .lastOr(Version.Unversioned)
      .map(SnapshotTableVersion)

  // Errors

  def unknownTableError(table: TableName): Exception = new Exception(s"Unknown table '${table.fullyQualifiedName}'")

  def unknownCommitId(id: CommitId): Exception = new Exception(s"Unknown commit ID '${id.id}'")

}
