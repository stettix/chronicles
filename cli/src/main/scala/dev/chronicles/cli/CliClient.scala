package dev.chronicles.cli

import java.time.Instant

import cats.effect.{Clock, Sync}
import cats.implicits._
import dev.chronicles.core.VersionTracker.TableOperation.{AddPartitionVersion, RemovePartition}
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core._

import scala.concurrent.duration.{MILLISECONDS => Millis}

/**
  * Implementation of the CLI commands, interacting with a VersionedMetastore to execute operations,
  * and showing results on the given Console.
  */
class CliClient[F[_]](delegate: VersionedMetastore[F], console: Console[F], clock: Clock[F])(implicit F: Sync[F]) {

  def executeAction(action: Action, userId: UserId): F[Unit] = action match {
    case Action.ListTables                                => listTables(console)
    case Action.InitTable(tableName, isSnapshot, message) => initTable(tableName, isSnapshot, userId, message)
    case Action.ListPartitions(tableName)                 => listPartitions(tableName)
    case Action.ShowTableHistory(tableName)               => showTableHistory(tableName)
    case Action.AddPartition(tableName, partitionName, message) =>
      addPartition(tableName, partitionName, userId, message)
    case Action.RemovePartition(tableName, partitionName, message) =>
      removePartition(tableName, partitionName, userId, message)
  }

  def listTables(console: Console[F]): F[Unit] =
    for {
      tables <- delegate.tables()
      tablesOutput = tables.map(_.fullyQualifiedName).mkString("\n")
      _ <- console.println(tablesOutput)
    } yield ()

  def listPartitions(table: TableName): F[Unit] = {
    def partitionsList(tableVersion: TableVersion): Either[Throwable, List[String]] = tableVersion match {
      case SnapshotTableVersion(_) => Left(new Error(s"Table $table is unpartitioned"))
      case PartitionedTableVersion(partitionVersions) =>
        Right(partitionVersions.map { case (partition, version) => s"$partition ${version.label}" }.toList)
    }

    for {
      tableVersion <- delegate.currentVersion(table)
      partitions <- F.fromEither(partitionsList(tableVersion))
      _ <- console.println(partitions.mkString("\n"))
    } yield ()
  }

  def initTable(
      name: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage): F[Unit] =
    for {
      now <- clock.realTime(Millis).map(Instant.ofEpochMilli)
      _ <- delegate.initTable(name, isSnapshot, userId, message, now)
      _ <- console.println(s"Initialised table ${name.fullyQualifiedName}")
    } yield ()

  def showTableHistory(tableName: TableName): F[Unit] =
    for {
      history <- delegate.updates(tableName)
      historyOutput = history.map(u => s"${u.id}\t${u.timestamp}\t${u.userId}\t${u.message}")
      _ <- console.println(historyOutput.mkString("\n"))
    } yield ()

  def addPartition(
      tableName: TableName,
      partitionName: String,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage): F[Unit] =
    for {
      now <- clock.realTime(Millis).map(Instant.ofEpochMilli)
      updateMetadata = VersionTracker.TableUpdateMetadata(userId, message, now)
      partitionVersion <- Version.generateVersion
      partition <- F.fromEither(Partition.parse(partitionName))
      updates = List(AddPartitionVersion(partition, partitionVersion))
      _ <- delegate.commit(tableName, VersionTracker.TableUpdate(updateMetadata, updates))
      _ <- console.println(s"Added partition '$partitionName' to table '${tableName.fullyQualifiedName}'")
    } yield ()

  def removePartition(
      tableName: TableName,
      partitionName: String,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage): F[Unit] =
    for {
      now <- clock.realTime(Millis).map(Instant.ofEpochMilli)
      updateMetadata = VersionTracker.TableUpdateMetadata(userId, message, now)
      partition <- F.fromEither(Partition.parse(partitionName))
      updates = List(RemovePartition(partition))
      _ <- delegate.commit(tableName, VersionTracker.TableUpdate(updateMetadata, updates))
      _ <- console.println(s"Added partition '$partitionName' to table '${tableName.fullyQualifiedName}'")
    } yield ()

}
