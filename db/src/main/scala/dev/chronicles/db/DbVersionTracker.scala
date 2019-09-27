package dev.chronicles.db

import dev.chronicles.core.{TableName, VersionTracker}
import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.implicits._

/**
  * This stores version history in a JDBC compatible database.
  */
class DbVersionTracker[F[_]](tx: Transactor[F])(implicit cs: ContextShift[F], F: Bracket[F, Throwable])
    extends VersionTracker[F] {

  /**
    * Create table schemas if not already present.
    */
  def init(): F[Unit] = {
    // TODO: Move these queries to resources? Or just constants? => The latter for compile time checks
    val createTablesTable =
      sql"""create table if not exists `chronicle_tables_v1` (
           |  metastore_id                varchar(32),
           |  table_name                  varchar(512),
           |  creation_time               timestamp not null,
           |  created_by                  varchar(32) not null,
           |  message                     varchar(4096) not null,
           |  is_snapshot_table           boolean not null,
           |  primary key (metastore_id, table_name)
           |)
           |""".stripMargin.update.run

    val createUpdatesTable =
      sql"""create table if not exists `chronicle_table_updates_v1` (
           |  commit_id                   varchar(36),
           |  metastore_id                varchar(32),
           |  table_name                  varchar(512),
           |  update_time                 timestamp not null,
           |  user_id                     varchar(32) not null,
           |  message                     varchar(4096) not null,
           |  primary key (commit_id),
           |  constraint fk_chronicle_table_updates_v1_to_tables foreign key (metastore_id, table_name) references chronicle_tables_v1(metastore_id, table_name)
           |)
           |""".stripMargin.update.run

    val createOperationsTable =
      sql"""create table if not exists `chronicle_table_operations_v1` (
           |  commit_id                   varchar(36),
           |  operation_type              enum('update_table_ver', 'add_part', 'update_part_ver', 'remove_part') not null,
           |  version                     varchar(62) not null,
           |  partition                   varchar(1024) not null,
           |  primary key (commit_id),
           |  constraint fk_chronicle_table_operations_v1_to_updates foreign key (commit_id) references chronicle_table_updates_v1(commit_id)
           |)
           |""".stripMargin.update.run

    val createVersionRefsTable =
      sql"""create table if not exists `chronicles_version_refs_v1` (
           |  metastore_id                varchar(32),
           |  table_name                  varchar(512),
           |  current_version             varchar(62) not null,
           |  constraint fk_chronicles_version_refs_v1_to_table foreign key (metastore_id, table_name) references chronicle_tables_v1(metastore_id, table_name)
           |)
           |""".stripMargin.update.run

    val createTables = createTablesTable >> createUpdatesTable >> createVersionRefsTable

    createTables.transact(tx).void
  }

  override def tables(): F[List[TableName]] =
    ???

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] =
    ???

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] =
    ???

  override protected def tableState(table: TableName): F[VersionTracker.TableState] =
    ???

  override protected def handleInit(table: TableName)(newTableState: => VersionTracker.TableState): F[Unit] =
    ???

}

object DbVersionTracker {}
