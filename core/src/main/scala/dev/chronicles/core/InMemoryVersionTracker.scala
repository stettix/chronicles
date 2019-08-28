package dev.chronicles.core

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import dev.chronicles.core.InMemoryVersionTracker._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core.util.RichRef._

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryVersionTracker[F[_]] private (allUpdates: Ref[F, TableUpdates])(implicit F: Sync[F])
    extends VersionTracker[F] {

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    val applyUpdate: TableUpdates => Either[Exception, TableUpdates] = { currentTableUpdates =>
      currentTableUpdates.get(table).fold[Either[Exception, TableUpdates]](Left(unknownTableError(table))) {
        currentTableState =>
          val newTableState =
            TableState(currentVersion = update.metadata.id, updates = currentTableState.updates :+ update)
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

  override def tableState(table: TableName): F[TableState] =
    for {
      allTableUpdates <- allUpdates.get
      tableState <- F.fromOption(allTableUpdates.get(table), unknownTableError(table))
    } yield tableState

  override def handleInit(table: TableName)(newTableState: => TableState): F[Unit] =
    allUpdates.update { prev =>
      if (prev.contains(table)) prev
      else {
        prev + (table -> newTableState)
      }
    }
}

object InMemoryVersionTracker {

  type TableUpdates = Map[TableName, TableState]

  /**
    * Safe constructor
    */
  def apply[F[_]](implicit F: Sync[F]): F[InMemoryVersionTracker[F]] =
    Ref[F].of(Map.empty[TableName, TableState]).map(new InMemoryVersionTracker[F](_))

}
