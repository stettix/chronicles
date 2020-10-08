package dev.chronicles.files.versiontracker

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}

import cats.effect.Sync
import cats.implicits._
import dev.chronicles.core.VersionTracker.TableOperation.InitTable
import dev.chronicles.core.VersionTracker.TableState.TimeAscending
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core.{TableName, VersionTracker}
import dev.chronicles.files.versiontracker.FileBackedVersionTracker._
import dev.chronicles.files.versiontracker.JsonCodecs._
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
class FileBackedVersionTracker[F[_]](fs: FileSystem, fsSyntax: FileSystemSyntax[F], rootDirectory: Path)(
    implicit F: Sync[F])
    extends VersionTracker[F] {

  // TODO: Only pass in FileSystem wrapper, not FileSystem + Syntax
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

    fs.createDirectory(tableDirectoryPath) >>
      fs.write(metadataPath, tableMetadataStr) >>
      commit(table, initialUpdate) >>
      setCurrentVersion(table, initialUpdate.metadata.id)

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

    (if (checkIfCommitExists) checkCommitExists else F.unit) >>
      fs.write(stateFilePath, stateFileContentJson, overwrite = true)
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

  final case class TableMetadataFile(isSnapshot: Boolean)
  final case class StateFile(headRef: String)

  private[versiontracker] val MetadataFilename = "table-metadata"
  private[versiontracker] val StateFilename = "head_ref"
  private[versiontracker] val TableDirectoryPrefix = "_chronicles_table_"
  private[versiontracker] val TableDirectoryPattern = s"$TableDirectoryPrefix(\\w+)\\.(\\w+)".r
  private[versiontracker] val TableUpdateFilePrefix = "table_update_"

  private[versiontracker] def parseTableName(directoryName: String): Option[TableName] = directoryName match {
    case TableDirectoryPattern(schemaName, tableName) => Some(TableName(schemaName, tableName))
    case _                                            => None
  }

  private val filenameDateFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS").withZone(ZoneId.of("UTC"))
  private[versiontracker] def tableUpdateFilename(timestamp: Instant): String =
    TableUpdateFilePrefix + filenameDateFormat.format(timestamp)

}
