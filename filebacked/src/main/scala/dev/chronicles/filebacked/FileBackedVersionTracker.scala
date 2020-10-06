package dev.chronicles.filebacked

import java.time.Instant

import cats.effect.Sync
import cats.implicits._
import dev.chronicles.core.VersionTracker.TableOperation.InitTable
import dev.chronicles.core.VersionTracker.{CommitId, TableState, TableUpdate, unknownTableError}
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

  def pathForTable(table: TableName): Path =
    new Path(rootDirectory, TableDirectoryPrefix + table.fullyQualifiedName)

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
      existsAlready <- fs.directoryExists(tableDirectoryPath) // TODO: Maybe better to try to read the existing file first?
      _ <- if (existsAlready) F.unit
      else fs.createDirectory(tableDirectoryPath) >> fs.write(metadataPath, tableMetadataStr)
      _ <- commit(table, initialUpdate)
    } yield ()
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    val tableDirectoryPath = pathForTable(table)

    // TODO: Create more readable and well defined format
    def generateTableUpdateFilename(timestamp: Instant): F[String] =
      F.delay(System.currentTimeMillis())
        .map(ts => TableUpdateFilePrefix + timestamp.toEpochMilli) // TODO: format timestamp better

    val writeTableUpdate: F[Unit] = for {
      timestamp <- timestamps.head.compile.toList
      ts <- F.fromOption(timestamp.headOption, new Error("Failed to get timestamp"))
      filename <- generateTableUpdateFilename(ts)
      pathForUpdate = new Path(tableDirectoryPath, filename)
      fileContent = update.asJson.printWith(spaces2)
      _ <- fs.write(pathForUpdate, fileContent)
      _ <- F.delay(println(s">>>>> Wrote table update to path $pathForUpdate: $fileContent"))
    } yield ()

    for {
      dirExists <- fs.directoryExists(tableDirectoryPath)
      _ <- if (!dirExists) { F.raiseError(unknownTableError(table)) } else F.unit
      _ <- writeTableUpdate
    } yield ()
  }

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] = {
    // TODO: Overwrite the reference state file to point to the given version
    //   * Check what checking we need to do on the given commit ID. <<-- What does that comment mean??
    F.unit // TODO!
  }

  override def isSnapshotTable(table: TableName): F[Boolean] = {
    val tableDirectoryPath = pathForTable(table)
    val metadataPath = new Path(tableDirectoryPath, MetadataFilename)

    for {
      tableMetadataStr <- fs.readString(metadataPath)
      tableMetadataJson <- F.fromEither(parse(tableMetadataStr))
      tableMetadata <- F.fromEither(tableMetadataJson.as[FileBackedVersionTracker.TableMetadataFile])
    } yield tableMetadata.isSnapshot
  }

  override protected def tableState[O <: TableState.Ordering](
      table: TableName,
      timeOrder: O): F[VersionTracker.TableState[F, O]] = {

    val tableDirectoryPath = pathForTable(table)

    // Read the current commit ID.
    // TODO!
    val currentVersion = CommitId("todo!")

    def isTableUpdate(file: Path): Boolean = file.getName.startsWith(TableUpdateFilePrefix)

    val sortOrder = if (timeOrder == TableState.TimeAscending) 1 else -1

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
      .evalTap(file => F.delay { println(s"*** Found update: $file"); file })
      .evalMap(fs.readString)
      .evalMap(content => F.fromEither(decode[TableUpdate](content)))

    F.delay(TableState(currentVersion, updates))
  }

}

object FileBackedVersionTracker {

  private[filebacked] val MetadataFilename = "table-metadata"
  private[filebacked] val TableDirectoryPrefix = "_chronicles_table_"
  private[filebacked] val TableDirectoryPattern = s"$TableDirectoryPrefix(\\w+)\\.(\\w+)".r
  private[filebacked] val TableUpdateFilePrefix = "table_update_"

  private[filebacked] def parseTableName(directoryName: String): Option[TableName] = directoryName match {
    case TableDirectoryPattern(schemaName, tableName) => Some(TableName(schemaName, tableName))
    case _                                            => None
  }

  final case class TableMetadataFile(isSnapshot: Boolean)

}
