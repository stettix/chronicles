package dev.chronicles.db

import java.time.Instant

import cats.effect._
import cats.implicits._
import dev.chronicles.core.{TableName, VersionTracker}
import doobie._
import doobie.implicits._

/**
  * This stores version history in a JDBC compatible database.
  */
class DbVersionTracker[F[_]](xa: Transactor[F])(implicit cs: ContextShift[F], F: Bracket[F, Throwable])
    extends VersionTracker[F] {

  /**
    * Create table schemas if not already present.
    */
  def init(): F[Unit] = {
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

    val createTables = createTablesTable >> createUpdatesTable >> createOperationsTable >> createVersionRefsTable

    createTables.transact(xa).void
  }

  override def tables(): F[List[TableName]] = {
    val rawTableNames = DbVersionTracker.allTablesQuery
      .query[String]
      .to[List]
      .transact(xa)

    for {
      tableNameStrings <- rawTableNames
      tableNames <- F.fromEither(tableNameStrings.map(TableName.fromFullyQualifiedName).sequence)
    } yield tableNames
  }

  override def init(
      table: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val tableExistsQuery: doobie.ConnectionIO[Long] =
      sql"""select count(*) from `chronicle_tables_v1` where table_name = ${table.fullyQualifiedName}"""
        .query[Long]
        .unique

    val addTableQuery: doobie.ConnectionIO[Int] = {
      val metastoreId = "default"
      sql"""insert into `chronicle_tables_v1` (metastore_id, table_name, creation_time, created_by, message, is_snapshot_table)
           |  values ($metastoreId, ${table.fullyQualifiedName}, $timestamp, ${userId.value}, ${message.content}, $isSnapshot)
           |  """.stripMargin.update.run
    }

    // Add table if it doesn't exist already
    val addIfNotExists = for {
      existingCount <- tableExistsQuery
      count <- if (existingCount == 0) addTableQuery else 42.pure[ConnectionIO]
    } yield count

    addIfNotExists.transact(xa).void
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] =
    ???

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] =
    ???

  override protected def tableState(table: TableName): F[VersionTracker.TableState] =
    ???

}

object DbVersionTracker {

  val allTablesQuery =
    sql"""select table_name from `chronicle_tables_v1`"""

}
