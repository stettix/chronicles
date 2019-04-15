package com.gu.tableversions.core

import java.time.Instant

import com.gu.tableversions.core.TableVersions.CommitResult

/**
  * This defines the interface for querying and updating table version information tracked by the system.
  */
trait TableVersions[F[_]] {

  /**
    * Start tracking version information for given table.
    * This must be called before any other operations can be performed on this table.
    */
  def init(table: TableName): F[Unit]

  /** Get details about partition versions in a table. */
  def currentVersion(table: TableName): F[TableVersion]

  /** Get a description of which version to write to next for the given partitions of a table. */
  def nextVersions(table: TableName, partitions: List[Partition]): F[List[PartitionVersion]]

  /**
    * Update partition versions to the given versions.
    * This performs no checking if data has been written to the associated paths but it will verify that these versions
    * 1) haven't been committed before and 2) these are the next versions to be committed for each of the partitions.
    */
  def commit(table: TableName, update: TableVersions.TableUpdate): F[CommitResult]

}

object TableVersions {

  /** A collection of updates to partitions to be applied and tracked as a single atomic change. */
  final case class TableUpdate(
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant,
      partitionUpdates: List[PartitionOperation])

  final case class UpdateMessage(content: String) extends AnyVal

  final case class UserId(value: String) extends AnyVal

  /** Result type for commit operation */
  sealed trait CommitResult

  object CommitResult {
    case object SuccessfulCommit extends CommitResult
    final case class InvalidCommit(invalidPartitions: Map[PartitionVersion, ErrorMessage]) extends CommitResult
  }

  case class ErrorMessage(value: String) extends AnyVal

  /** ADT for operations on individual partitions. */
  sealed trait PartitionOperation

  object PartitionOperation {
    final case class AddPartitionVersion(version: PartitionVersion) extends PartitionOperation
    final case class RemovePartition(partition: Partition) extends PartitionOperation
  }

}
