package dev.chronicles.cli

import cats.effect.Sync
import dev.chronicles.core._

/**
  * Temporary placeholder for real metastore clients.
  */
class StubMetastore[F[_]](implicit F: Sync[F]) extends Metastore[F] {
  override def currentVersion(table: TableName): F[TableVersion] = F.raiseError(new Error("Not implemented"))
  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] =
    F.raiseError(new Error("Not implemented"))
}
