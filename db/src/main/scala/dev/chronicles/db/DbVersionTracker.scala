package dev.chronicles.db

import java.time.Instant

import cats.effect._
import cats.implicits._
import cats.kernel.Eq
import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core._
import doobie._
import doobie.implicits._
import fs2.{Chunk, Stream}

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
           |  init_commit_id              varchar(36),
           |  creation_time               timestamp not null,
           |  created_by                  varchar(32) not null,
           |  message                     varchar(4096) not null,
           |  is_snapshot_table           boolean not null,
           |  primary key (metastore_id, table_name)
           |)
           |""".stripMargin.update.run

    val createUpdatesTable =
      sql"""create table if not exists `chronicle_table_updates_v1` (
           |  sequence_id                 long generated always as identity,
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
           |  index_in_commit             int not null,
           |  operation_type              enum('update_table_version', 'add_part_version', 'remove_part') not null,
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

  override def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(InitTable(table, isSnapshot)))

    val addTableQuery = {
      val metastoreId = "default"
      sql"""insert into `chronicle_tables_v1` (metastore_id, table_name, init_commit_id, creation_time, created_by, message, is_snapshot_table)
           |  values ($metastoreId, ${table.fullyQualifiedName}, ${initialUpdate.metadata.id}, $timestamp, ${userId.value}, ${message.content}, $isSnapshot)
           |  """.stripMargin.update.run.void
    }

    // Add table if it doesn't exist already
    val addIfNotExists = for {
      existingCount <- DbVersionTracker.tableExistsQuery(table)
      _ <- if (existingCount == 0) addTableQuery else Unit.pure[ConnectionIO]
    } yield ()

    addIfNotExists.transact(xa).void
  }

  override protected def tableState(table: TableName): F[VersionTracker.TableState] = {
    // First get the metadata for the 'init' operation, by querying table metadata.
    // Make this query produce an error if the table isn't found, using the 'unique' operation on the query.

    def tableMetadataQuery(table: TableName): doobie.ConnectionIO[(String, Instant, String, String, Boolean)] =
      sql"""
           |select init_commit_id, creation_time, created_by, message, is_snapshot_table
           |  from chronicle_tables_v1
           |  where table_name = ${table.fullyQualifiedName}
           |""".stripMargin.query[(String, Instant, String, String, Boolean)].unique

    val initialTableUpdate: ConnectionIO[TableUpdate] = for {
      (initCommitId, creationTime, createdBy, message, isSnapshot) <- tableMetadataQuery(table)
      metadata = TableUpdateMetadata(CommitId(initCommitId), UserId(createdBy), UpdateMessage(message), creationTime)
    } yield TableUpdate(metadata, operations = List(InitTable(table, isSnapshot)))

    // The query produces the value for a TableUpdateMetadata and TableOperation for each row.
    // Chunk these up by grouping the resulting stream by adjaceent TableUpdateMetadata object.

    def toUpdate(
        commitId: String,
        creationTime: Instant,
        createdBy: String,
        message: String,
        operationType: String,
        versionStr: String,
        partitionStr: String
    ): Either[Throwable, TableUpdate] =
      for {
        version <- Version.parse(versionStr)
        partition <- Partition.parse(partitionStr)
        operation <- typedOperation(operationType, version, partition)
        metadata = TableUpdateMetadata(CommitId(commitId), UserId(createdBy), UpdateMessage(message), creationTime)
      } yield TableUpdate(metadata, operations = List(operation))

    val updatesQuery =
      sql"""select
      |       u.commit_id, u.update_time, u.user_id, u.message,
      |       o.operation_type, o.version, o.partition
      |     from chronicle_tables_v1 t
      |       inner join chronicle_table_updates_v1 u
      |     on t.metastore_id = u.metastore_id and t.table_name = u.table_name
      |       inner join chronicle_table_operations_v1 o
      |     on u.commit_id = o.commit_id
      |     where t.table_name = ${table.fullyQualifiedName}
      |     order by u.sequence_id, o.index_in_commit
      |""".stripMargin
        .query[(String, Instant, String, String, String, String, String)]

    val updatesStream: Stream[doobie.ConnectionIO, (TableUpdateMetadata, List[TableOperation])] =
      updatesQuery.stream
        .map((toUpdate _).tupled)
        .flatMap(Stream.fromEither[ConnectionIO](_))
        .groupAdjacentBy(_.metadata)(Eq.fromUniversalEquals[TableUpdateMetadata])
        .map { case (metadata, updates) => metadata -> updates.toList.flatMap(_.operations) }

    val allUpdatesStream =
      Stream.eval(initialTableUpdate).map(update => update.metadata -> update.operations) ++ updatesStream

    def currentVersionQuery: ConnectionIO[Option[CommitId]] =
      sql"""select current_version
           |  from `chronicles_version_refs_v1`
           |  where table_name = ${table.fullyQualifiedName}
           |""".stripMargin.query[String].option.map(_.map(CommitId))

    val tableState = for {
      initialUpdate <- initialTableUpdate
      currentVersion <- currentVersionQuery
      updates <- allUpdatesStream.compile.toList
      tableUpdates = updates.map { case (metadata, updates) => TableUpdate(metadata, updates) }
    } yield TableState(currentVersion.getOrElse(initialUpdate.metadata.id), tableUpdates)

    tableState.transact(xa)
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] =
    ???

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] =
    ???

  private def typedOperation(op: String, version: Version, partition: Partition): Either[Throwable, TableOperation] =
    op match {
      case "update_table_version" => AddTableVersion(version).asRight
      case "add_part_version"     => AddPartitionVersion(partition, version).asRight
      case "remove_part"          => RemovePartition(partition).asRight
      case _                      => new Error(s"Invalid operation type '$op'").asLeft
    }

}

object DbVersionTracker {

  val allTablesQuery =
    sql"""select table_name from `chronicle_tables_v1`"""

  def tableExistsQuery(table: TableName) =
    sql"""select count(*) from `chronicle_tables_v1` where table_name = ${table.fullyQualifiedName}"""
      .query[Long]
      .unique

}
