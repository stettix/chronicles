package dev.chronicles.core

import java.time.Instant

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import dev.chronicles.core.InMemoryVersionTracker._
import dev.chronicles.core.VersionTracker.TableOperation.InitTable
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core.util.RichRef._
import fs2.Stream

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryVersionTracker[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends VersionTracker[F] {

  override def tables(): Stream[F, TableName] =
    for {
      allTableUpdates <- Stream.eval(allUpdates.get)
      updates <- Stream.emits(allTableUpdates.keys.toList)
    } yield updates

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      currentTableUpdates.get(table).fold[Either[Exception, TableUpdates]](Left(unknownTableError(table))) {
        currentTableState =>
          val newTableState =
            InMemoryTableState(currentVersion = update.metadata.id, updates = currentTableState.updates :+ update)
          val updatedStates = currentTableUpdates + (table -> newTableState)
          Right(updatedStates)
      }
    }

    allUpdates.modifyEither(applyUpdate)
  }

  override def setCurrentVersion(table: TableName, id: CommitId): F[Unit] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      currentTableUpdates.get(table).fold[Either[Exception, TableUpdates]](Left(unknownTableError(table))) {
        currentTableState =>
          if (currentTableState.updates.exists(_.metadata.id == id)) {
            val newTableState = currentTableState.copy(currentVersion = id)
            val updatedStates = currentTableUpdates + (table -> newTableState)
            Right(updatedStates)
          } else
            Left(unknownCommitId(id))
      }
    }

    allUpdates.modifyEither(applyUpdate)
  }

  override def tableState(table: TableName, timeOrder: Boolean): F[TableState[F]] =
    for {
      allTableUpdates <- allUpdates.get
      tableState <- F.fromOption(allTableUpdates.get(table), unknownTableError(table))
      updates = if (timeOrder) tableState.updates else tableState.updates.reverse
    } yield TableState(tableState.currentVersion, Stream.emits(updates))

  override def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: UserId,
      message: UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(InitTable(table, isSnapshot)))
    def newTableState = InMemoryTableState(currentVersion = initialUpdate.metadata.id, updates = List(initialUpdate))

    allUpdates.update { prev =>
      if (prev.contains(table))
        prev
      else
        prev + (table -> newTableState)
    }
  }

  override def isSnapshotTable(table: TableName): F[Boolean] = {
    def isSnapshot(operations: List[TableOperation]): Boolean = operations match {
      case InitTable(_, isSnapshot) :: _ => isSnapshot
      case _                             => false
    }

    for {
      tableUpdates <- allUpdates.get
      tableState <- F.fromOption(tableUpdates.get(table), unknownTableError(table))
      initialUpdate <- F.fromOption(tableState.updates.headOption,
                                    new Exception(s"Invalid state for table $table, no updates found"))
    } yield isSnapshot(initialUpdate.operations)
  }

}

object InMemoryVersionTracker {

  final case class InMemoryTableState(currentVersion: CommitId, updates: List[TableUpdate])

  type TableUpdates = Map[TableName, InMemoryTableState]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryVersionTracker[F]] =
    Ref[F].of(Map.empty[TableName, InMemoryTableState]).map(new InMemoryVersionTracker[F](_))

}
