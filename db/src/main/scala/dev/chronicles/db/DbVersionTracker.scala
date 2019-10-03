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
import fs2.Stream

/**
  * This stores version history in a JDBC compatible database.
  */
class DbVersionTracker[F[_]](xa: Transactor[F])(implicit cs: ContextShift[F], F: Bracket[F, Throwable])
    extends VersionTracker[F] {

  import DbVersionTracker._

  /**
    * Create table schemas if not already present.
    */
  def init(): F[Unit] =
    initTables(xa)

  override def tables(): F[List[TableName]] = {
    val rawTableNames = DbVersionTracker.getAllTables
      .to[List]
      .transact(xa)

    for {
      tableNameStrings <- rawTableNames
      tableNames <- F.fromEither(tableNameStrings.traverse(TableName.fromFullyQualifiedName))
    } yield tableNames
  }

  override def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val initOperation = InitTable(table, isSnapshot)
    val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(initOperation))

    val insertInitialState = List(
      addTable(table, initialUpdate.metadata.id, timestamp, userId, message, isSnapshot),
      addTableUpdate(initialUpdate.metadata.id, table, timestamp, userId, message),
      updateForOperation(initialUpdate.metadata.id, initOperation, 0),
      initialiseCurrentVersion(table, initialUpdate.metadata.id)
    ).traverse(_.run.void)

    // Add table if it doesn't exist already
    val addTableIfNotExists = for {
      tableMetadata <- getTableMetadata(table).option
      _ <- if (tableMetadata.isEmpty) insertInitialState else Unit.pure[ConnectionIO]
    } yield ()

    addTableIfNotExists.transact(xa).void
  }

  override protected def tableState(table: TableName): F[VersionTracker.TableState] = {

    def toTableUpdate(
        commitId: String,
        creationTime: Instant,
        createdBy: String,
        message: String,
        operationType: String,
        versionStr: Option[String],
        partitionStr: Option[String],
        tableNameStr: Option[String],
        isSnapshot: Option[Boolean]
    ): Either[Throwable, TableUpdate] = {
      for {
        version <- versionStr.traverse(Version.parse)
        partition <- partitionStr.traverse(Partition.parse)
        tableName <- tableNameStr.traverse(TableName.fromFullyQualifiedName)
        operation <- typedOperation(operationType, version, partition, tableName, isSnapshot)
        metadata = TableUpdateMetadata(CommitId(commitId), UserId(createdBy), UpdateMessage(message), creationTime)
      } yield TableUpdate(metadata, operations = List(operation))
    }

    // The query produces the value for a TableUpdateMetadata and TableOperation for each row.
    // Chunk these up by grouping the resulting stream by adjacent TableUpdateMetadata object.
    val updatesStream =
      getUpdates(table).stream
        .map((toTableUpdate _).tupled)
        .flatMap(Stream.fromEither[ConnectionIO](_))
        .groupAdjacentBy(_.metadata)(Eq.fromUniversalEquals[TableUpdateMetadata])
        .map { case (metadata, updates) => metadata -> updates.toList.flatMap(_.operations) }

    val tableState = for {
      maybeCurrentVersion <- getCurrentVersion(table).option
      currentVersion <- liftOrError(maybeCurrentVersion, unknownTableError(table))
      updates <- updatesStream.compile.toList
      tableUpdates = updates.map { case (metadata, updates) => TableUpdate(metadata, updates) }
    } yield TableState(currentVersion, tableUpdates)

    tableState.transact(xa)
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    import update.metadata._

    val tableUpdate = addTableUpdate(id, table, timestamp, userId, message)
    val currentVersionUpdate = updateCurrentVersion(table, id)
    val operations = updatesForOperations(id, update.operations)

    val performUpdates = (tableUpdate :: currentVersionUpdate :: operations).traverse(_.run)

    val io = for {
      t <- getTableMetadata(table).map(_._1).option
      _ <- liftOrError(t, unknownTableError(table))

      _ <- performUpdates
    } yield ()

    io.transact(xa).void
  }

  override def setCurrentVersion(table: TableName, commitId: VersionTracker.CommitId): F[Unit] = {
    val io = for {
      t <- getTableMetadata(table).map(_._1).option
      _ <- liftOrError(t, unknownTableError(table))

      c <- getCommit(commitId).option
      _ <- liftOrError(c, unknownCommitId(commitId))

      _ <- updateCurrentVersion(table, commitId).run
    } yield ()

    io.transact(xa)
  }

}

object DbVersionTracker {

  def liftOrError[A](a: Option[A], error: => Throwable): ConnectionIO[A] =
    a.map(_.pure[ConnectionIO]).getOrElse(error.raiseError[ConnectionIO, A])

  def initTables[F[_]](xa: Transactor[F])(implicit cs: ContextShift[F], F: Bracket[F, Throwable]): F[Unit] = {

    val createTables =
      createTablesTable.run *>
        createUpdatesTable.run *>
        createOperationsTable.run *>
        createVersionRefsTable.run

    createTables.transact(xa).void
  }

  val createTablesTable =
    sql"""create table if not exists `chronicle_tables_v1` (
         |  metastore_id                varchar(32),
         |  table_name                  varchar(512),
         |  init_commit_id              varchar(36) not null,
         |  creation_time               timestamp not null,
         |  created_by                  varchar(32) not null,
         |  message                     varchar(4096) not null,
         |  is_snapshot_table           boolean not null,
         |  primary key (metastore_id, table_name)
         |)
         |""".stripMargin.update

  val createUpdatesTable =
    sql"""create table if not exists `chronicle_table_updates_v1` (
         |  sequence_id                 long generated always as identity,
         |  commit_id                   varchar(36) not null,
         |  metastore_id                varchar(32) not null,
         |  table_name                  varchar(512) not null,
         |  update_time                 timestamp not null,
         |  user_id                     varchar(32) not null,
         |  message                     varchar(4096) not null,
         |  primary key (commit_id),
         |  constraint fk_chronicle_table_updates_v1_to_tables foreign key (metastore_id, table_name) references chronicle_tables_v1(metastore_id, table_name)
         |)
         |""".stripMargin.update

  val createOperationsTable =
    sql"""create table if not exists `chronicle_table_operations_v1` (
         |  commit_id                   varchar(36),
         |  index_in_commit             int not null,
         |  operation_type              varchar(20) not null,
         |  version                     varchar(62),
         |  partition                   varchar(1024),
         |  table_name                  varchar(512),
         |  is_snapshot_table           boolean,
         |  primary key (commit_id, index_in_commit),
         |  constraint fk_chronicle_table_operations_v1_to_updates foreign key (commit_id) references chronicle_table_updates_v1(commit_id)
         |)
         |""".stripMargin.update

  val createVersionRefsTable =
    sql"""create table if not exists `chronicles_version_refs_v1` (
         |  metastore_id                varchar(32),
         |  table_name                  varchar(512),
         |  current_version             varchar(62) not null,
         |  primary key (metastore_id, table_name),
         |  constraint fk_chronicles_version_refs_v1_to_table foreign key (metastore_id, table_name) references chronicle_tables_v1(metastore_id, table_name)
         |)
         |""".stripMargin.update

  val getAllTables =
    sql"""select table_name from `chronicle_tables_v1`"""
      .query[String]

  def addTable(
      table: TableName,
      commitId: CommitId,
      createTime: Instant,
      userId: UserId,
      message: UpdateMessage,
      isSnapshot: Boolean) =
    sql"""insert into `chronicle_tables_v1` (metastore_id, table_name, init_commit_id, creation_time, created_by, message, is_snapshot_table)
         |  values ('default', ${table.fullyQualifiedName}, ${commitId.id}, $createTime, ${userId.value}, ${message.content}, $isSnapshot)
         |  """.stripMargin.update

  def addTableUpdate(
      commitId: CommitId,
      table: TableName,
      updateTime: Instant,
      userId: UserId,
      message: UpdateMessage
  ) =
    sql"""insert into `chronicle_table_updates_v1` (commit_id, metastore_id, table_name, update_time, user_id, message)
         |  values (${commitId.id}, 'default', ${table.fullyQualifiedName}, $updateTime, ${userId.value}, ${message.content})
         |""".stripMargin.update

  def addOperation(
      commitId: CommitId,
      indexInCommit: Int,
      operationType: String,
      version: Option[Version],
      partition: Option[Partition],
      tableName: Option[TableName] = None,
      isSnapshot: Option[Boolean] = None
  ) =
    sql"""insert into `chronicle_table_operations_v1` (commit_id, index_in_commit, operation_type, version, partition, table_name, is_snapshot_table)
         |  values (${commitId.id}, $indexInCommit, $operationType, ${version.map(_.label)}, ${partition.map(_.toString)}, ${tableName
           .map(_.fullyQualifiedName)}, $isSnapshot)
         |""".stripMargin.update

  def getTableMetadata(table: TableName) =
    sql"""
         |select init_commit_id, creation_time, created_by, message, is_snapshot_table
         |  from chronicle_tables_v1
         |  where table_name = ${table.fullyQualifiedName}
         |""".stripMargin.query[(String, Instant, String, String, Boolean)]

  def getUpdates(table: TableName) =
    sql"""select
         |    u.commit_id, u.update_time, u.user_id, u.message,
         |    o.operation_type, o.version, o.partition, o.table_name, o.is_snapshot_table
         |  from chronicle_tables_v1 t
         |    inner join chronicle_table_updates_v1 u
         |  on t.metastore_id = u.metastore_id and t.table_name = u.table_name
         |    inner join chronicle_table_operations_v1 o
         |  on u.commit_id = o.commit_id
         |  where t.table_name = ${table.fullyQualifiedName}
         |  order by u.sequence_id, o.index_in_commit
         |""".stripMargin
      .query[(String, Instant, String, String, String, Option[String], Option[String], Option[String], Option[Boolean])]

  def getCurrentVersion(table: TableName) =
    sql"""select current_version
         |  from `chronicles_version_refs_v1`
         |  where table_name = ${table.fullyQualifiedName}
         |""".stripMargin.query[String].map(CommitId)

  def initialiseCurrentVersion(table: TableName, commitId: CommitId) =
    sql"""insert into `chronicles_version_refs_v1` (metastore_id, table_name, current_version)
         |  values('default', ${table.fullyQualifiedName}, ${commitId.id})
         |""".stripMargin.update

  def updateCurrentVersion(table: TableName, commitId: CommitId) =
    sql"""update`chronicles_version_refs_v1`
         |  set `current_version` = ${commitId.id}
         |  where table_name = ${table.fullyQualifiedName}
         |""".stripMargin.update

  def getCommit(commitId: CommitId) =
    sql"""select table_name, update_time, user_id, message
         |  from `chronicle_table_updates_v1`
         |  where commit_id = ${commitId.id}
         |""".stripMargin.query[(String, Instant, String, String)]

  private def typedOperation(
      op: String,
      version: Option[Version],
      partition: Option[Partition],
      tableName: Option[TableName],
      isSnapshot: Option[Boolean]): Either[Throwable, TableOperation] =
    (op, version, partition, tableName, isSnapshot) match {
      case ("add_table_version", Some(version), _, _, _)              => AddTableVersion(version).asRight
      case ("add_part_version", Some(version), Some(partition), _, _) => AddPartitionVersion(partition, version).asRight
      case ("remove_part", _, Some(partition), _, _)                  => RemovePartition(partition).asRight
      case ("init_table", _, _, Some(table), Some(snapshot))          => InitTable(table, snapshot).asRight
      case _                                                          => new Error(s"Found invalid operation '$op', version=$version, partition=$partition").asLeft
    }

  private def updatesForOperations(commitId: CommitId, operations: List[TableOperation]): List[Update0] =
    operations.zipWithIndex.map { case (op, idx) => updateForOperation(commitId, op, idx) }

  private def updateForOperation(commitId: CommitId, operation: TableOperation, index: Int): Update0 =
    operation match {
      case AddTableVersion(version) =>
        addOperation(commitId, index, "add_table_version", Some(version), None)
      case AddPartitionVersion(partition, version) =>
        addOperation(commitId, index, "add_part_version", Some(version), Some(partition))
      case RemovePartition(partition) =>
        addOperation(commitId, index, "remove_part", None, Some(partition))
      case InitTable(tableName, isSnapshot) =>
        addOperation(commitId, index, "init_table", None, None, Some(tableName), Some(isSnapshot))
    }

}
