package dev.chronicles.cli

import cats.effect.Sync
import dev.chronicles.core.{TableName, VersionTracker, VersionedMetastore}
import cats.implicits._

class VersionRepositoryClient[F[_]](delegate: VersionedMetastore[F], console: Console[F], clock: Clock[F])(
    implicit F: Sync[F]) {

  def executeAction(action: Action): F[Unit] = action match {
    case Action.ListTables                                        => listTables(console)
    case Action.InitTable(tableName, isSnapshot, userId, message) => initTable(tableName, isSnapshot, userId, message)
    case Action.ListPartitions(tableName)                         => listPartitions(tableName)
    case Action.ShowTableHistory(tableName)                       => ???
    case Action.AddPartition(tableName, partitionName)            => ???
    case Action.RemovePartition(tableName, partitionName)         => ???
  }

  def listTables(console: Console[F]): F[Unit] = {

    //delegate.versionTracker. // OOPS! No suitable method to call!!!

    ???
  }

  def listPartitions(tableName: TableName): F[Unit] = {
    ???

  }

  def initTable(
      name: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage): F[Unit] =
    for {
      now <- clock.now
      _ <- delegate.init(name, isSnapshot, userId, message, now)
      _ <- console.println(s"Initialised table ${name.fullyQualifiedName}")
    } yield ()

}
// TODO:
//   - Dummy implementation using memory store
//   - Implementation using REST client
//   Common code for these will live in the trait, and be tested via generic tests for this.
