package dev.chronicles.filebacked

import java.time.Instant

import cats.effect.Sync
import cats.implicits._
import dev.chronicles.core.VersionTracker.TableOperation.InitTable
import dev.chronicles.core.VersionTracker.TableState.TimeAscending
import dev.chronicles.core.VersionTracker.{CommitId, TableState, TableUpdate, unknownCommitId, unknownTableError}
import dev.chronicles.core.{TableName, VersionTracker}
import dev.chronicles.filebacked.FileBackedVersionTracker._
import dev.chronicles.filebacked.JsonCodecs._
import fs2.Stream
import io.circe.Printer.spaces2
import io.circe.parser._
import io.circe.syntax._
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * This class implementation a VersionTracker that stores its state in an underlying filesystem.
  *
  * @param rootDirectory Root directory for table metadata. This does not have to be in the same location as table data.
  */
class FileBackedVersionTracker[F[_]](fs: FileSystem, rootDirectory: Path)(implicit F: Sync[F])
    extends VersionTracker[F] {

  private val fsSyntax = FileSystemSyntax()
  import fsSyntax._

  private val timestamps: Stream[F, Instant] = Timestamps.uniqueTimestamps

  override def tables(): Stream[F, TableName] = {
    val folders = fs.listDirectories(rootDirectory)

    Stream
      .eval(folders)
      .flatMap(Stream.emits(_))
      .mapFilter(path => parseTableName(path.getName))
  }

  override def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val tableDirectoryPath = pathForTable(table)
    val metadataPath = new Path(tableDirectoryPath, MetadataFilename)
    val tableMetadataStr = TableMetadataFile(isSnapshot).asJson.printWith(spaces2)
    val initialUpdate = TableUpdate(userId, message, timestamp, operations = List(InitTable(table, isSnapshot)))

    for {
      existsAlready <- fs.directoryExists(tableDirectoryPath)
      _ <- if (existsAlready) F.unit
      else fs.createDirectory(tableDirectoryPath) >> fs.write(metadataPath, tableMetadataStr)
      _ <- commit(table, initialUpdate)
      _ <- setCurrentVersion(table, initialUpdate.metadata.id)
    } yield ()
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    val tableDirectoryPath = pathForTable(table)

    // TODO: Create more readable and well defined format, with tests
    def tableUpdateFilename(timestamp: Instant): String =
      TableUpdateFilePrefix + timestamp.toEpochMilli // TODO: format timestamp better

    val writeTableUpdate: F[Unit] = for {
      _ <- checkExists(table)
      timestamp <- timestamps.head.compile.toList
      ts <- F.fromOption(timestamp.headOption, new Error("Failed to get timestamp"))
      pathForUpdate = new Path(tableDirectoryPath, tableUpdateFilename(ts))
      fileContent = update.asJson.printWith(spaces2)
      _ <- fs.write(pathForUpdate, fileContent)
      _ <- setCurrentVersion(table, update.metadata.id, checkIfCommitExists = false)
    } yield ()

    for {
      dirExists <- fs.directoryExists(tableDirectoryPath)
      _ <- if (!dirExists) F.raiseError(unknownTableError(table)) else F.unit
      _ <- writeTableUpdate
    } yield ()
  }

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] =
    setCurrentVersion(table, id, checkIfCommitExists = true)

  override def isSnapshotTable(table: TableName): F[Boolean] = {
    val tableDirectoryPath = pathForTable(table)
    val metadataPath = new Path(tableDirectoryPath, MetadataFilename)

    for {
      _ <- checkExists(table)
      tableMetadataStr <- fs.readString(metadataPath)
      tableMetadataJson <- F.fromEither(parse(tableMetadataStr))
      tableMetadata <- F.fromEither(tableMetadataJson.as[FileBackedVersionTracker.TableMetadataFile])
    } yield tableMetadata.isSnapshot
  }

  override protected def tableState[O <: TableState.Ordering](
      table: TableName,
      timeOrder: O): F[VersionTracker.TableState[F, O]] = {

    val tableDirectoryPath = pathForTable(table)
    val stateFilePath = new Path(tableDirectoryPath, StateFilename)

    val readCurrentVersion = for {
      _ <- checkExists(table)
      stateFileContent <- fs.readString(stateFilePath)
      stateFile <- F.fromEither(decode[StateFile](stateFileContent))
    } yield CommitId(stateFile.headRef)

    def isTableUpdate(file: Path): Boolean = file.getName.startsWith(TableUpdateFilePrefix)

    // Read sorted sequence of table update files.
    val tableUpdateFiles = for {
      allFiles <- fs.listFiles(tableDirectoryPath)
      tableUpdateFiles = allFiles.filter(file => isTableUpdate(file.getPath))
      sortedUpdates = tableUpdateFiles.sortBy(f => f.getModificationTime -> f.getPath.getName)
      updates = if (timeOrder == TableState.TimeAscending) sortedUpdates else sortedUpdates.reverse
    } yield updates.map(_.getPath)

    // Convert files.
    val updates = Stream
      .eval(tableUpdateFiles)
      .flatMap(files => Stream.emits(files))
      .evalMap(fs.readString)
      .evalMap(content => F.fromEither(decode[TableUpdate](content)))

    readCurrentVersion
      .map(currentState => TableState(currentState, updates))
  }

  private def setCurrentVersion(
      table: TableName,
      id: VersionTracker.CommitId,
      checkIfCommitExists: Boolean): F[Unit] = {
    val tableDirectoryPath = pathForTable(table)
    val stateFilePath = new Path(tableDirectoryPath, StateFilename)
    val stateFileContent = StateFile(id.id)
    val stateFileContentJson = stateFileContent.asJson.printWith(spaces2)

    val checkCommitExists = for {
      _ <- checkExists(table)
      state <- tableState(table, TimeAscending)
      commits <- state.updates.find(_.metadata.id == id).head.compile.toList
      _ <- if (commits.nonEmpty) F.unit else F.raiseError(unknownCommitId(id))
    } yield ()

    //  To avoid having to do this in the internal call to setCurrentVersion, pull out the non-check part into a separate private method.
    for {
      _ <- if (checkIfCommitExists) checkCommitExists else F.unit
      _ <- fs.write(stateFilePath, stateFileContentJson, overwrite = true)
    } yield ()
  }

  private def pathForTable(table: TableName): Path =
    new Path(rootDirectory, TableDirectoryPrefix + table.fullyQualifiedName)

  private def checkExists(table: TableName): F[Unit] = {
    val tableDirectoryPath = pathForTable(table)
    for {
      exists <- fs.directoryExists(tableDirectoryPath)
      _ <- if (exists) F.unit else F.raiseError(unknownTableError(table))
    } yield ()
  }

}

object FileBackedVersionTracker {

  // TODO: Make (some of) these public?
  private[filebacked] val MetadataFilename = "table-metadata"
  private[filebacked] val StateFilename = "head_ref"
  private[filebacked] val TableDirectoryPrefix = "_chronicles_table_"
  private[filebacked] val TableDirectoryPattern = s"$TableDirectoryPrefix(\\w+)\\.(\\w+)".r
  private[filebacked] val TableUpdateFilePrefix = "table_update_"

  private[filebacked] def parseTableName(directoryName: String): Option[TableName] = directoryName match {
    case TableDirectoryPattern(schemaName, tableName) => Some(TableName(schemaName, tableName))
    case _                                            => None
  }

  final case class TableMetadataFile(isSnapshot: Boolean)

  final case class StateFile(headRef: String)

}
