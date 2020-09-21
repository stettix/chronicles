package dev.chronicles.filebacked

import java.time.Instant

import cats.effect.Sync
import dev.chronicles.core.{TableName, VersionTracker}
import dev.chronicles.core.VersionTracker.{CommitId, TableState}
import fs2.Stream
import org.apache.hadoop.fs.{FileSystem, Path}
import cats.implicits._
import dev.chronicles.filebacked.FileBackedVersionTracker._

case class TableMetadataFile(isSnapshot: Boolean)

/**
  * This class implementation a VersionTracker that stores its state in an underlying filesystem.
  *
  * @param rootFolder Root folder for table metadata. Not that actual table data can live elsewhere.
  */
class FileBackedVersionTracker[F[_]](fs: FileSystem, rootFolder: Path)(implicit F: Sync[F]) extends VersionTracker[F] {

  val fsSyntax = FileSystemSyntax()
  import fsSyntax._

  override def tables(): Stream[F, TableName] = {
    val folders = fs.listDirectories(rootFolder)

    Stream
      .eval(folders)
      .flatMap(Stream.emits(_))
      .mapFilter(path => parseTableName(path.getName))
  }

  def pathForTable(table: TableName): Path =
    new Path(rootFolder, TableFolderPrefix + table.fullyQualifiedName)

  override def initTable(
      table: TableName,
      isSnapshot: Boolean,
      userId: VersionTracker.UserId,
      message: VersionTracker.UpdateMessage,
      timestamp: Instant): F[Unit] = {

    val tableFolderPath = pathForTable(table)
    val metadataPath = new Path(tableFolderPath, MetadataFilename)

    val tableMetadataStr = s"""{"is_snapshot": $isSnapshot }""" // TODO! Use a typed value and encode using Circe. Maybe include extra metadata fields too.

    for {
      existsAlready <- fs.directoryExists(tableFolderPath) // TODO: Maybe better to try to read the existing file first?
      _ <- if (existsAlready) F.unit
      else fs.createDirectory(tableFolderPath) >> fs.write(metadataPath, tableMetadataStr)
    } yield ()
  }

  override def commit(table: TableName, update: VersionTracker.TableUpdate): F[Unit] = {
    // TODO: Write a new entry with a table update
    //   Fail with unknownTableError if the table directory doesn't exist
    F.unit // TODO!
  }

  override def setCurrentVersion(table: TableName, id: VersionTracker.CommitId): F[Unit] = {
    // TODO: Overwrite the reference state file to point to the given version
    //   * Check what checking we need to do on the given commit ID. <<-- What does that comment mean??
    F.unit // TODO!
  }

  override def isSnapshotTable(table: TableName): F[Boolean] = {
    val tableFolderPath = pathForTable(table)
    val metadataPath = new Path(tableFolderPath, MetadataFilename)
    for {
      tableMetadata <- fs.readString(metadataPath)
    } yield tableMetadata.contains("true") // TODO: Parse to object!!!
  }

  override protected def tableState[O <: TableState.Ordering](
      table: TableName,
      timeOrder: O): F[VersionTracker.TableState[F, O]] = {
    // TODO: Read the table updates for the requested table
    //   Then sort them in the requested order (I suspect I can't request files in a sorted order from a Hadoop FileSystem)

    F.pure(TableState(CommitId("todo!"), Stream.empty)) // TODO!
  }

}

object FileBackedVersionTracker {

  private[filebacked] val MetadataFilename = "table-metadata"
  private[filebacked] val TableFolderPrefix = "_chronicles_table_"
  private[filebacked] val TableFolderPattern = s"$TableFolderPrefix(\\w+)\\.(\\w+)".r

  private[filebacked] def parseTableName(folderName: String): Option[TableName] = folderName match {
    case TableFolderPattern(schemaName, tableName) => Some(TableName(schemaName, tableName))
    case _                                         => None
  }

}
