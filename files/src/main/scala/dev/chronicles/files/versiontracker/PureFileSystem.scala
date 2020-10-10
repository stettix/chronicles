package dev.chronicles.files.versiontracker

import java.io.{InputStream, OutputStream}

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import fs2._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
  * This implements a facade for a Hadoop FileSystem that provides an API based on Cats Effect.
  */
case class PureFileSystem[F[_]](fs: FileSystem, blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]) {

  def directoryExists(path: Path): F[Boolean] = F.delay(fs.exists(path))

  def createDirectory(path: Path): F[Unit] = F.delay(fs.mkdirs(path)).void

  def readString(path: Path): F[String] = {
    val inputStream: F[InputStream] = F.delay(fs.open(path))

    io.readInputStream(inputStream, chunkSize = 4096, blocker = blocker, closeAfterUse = true)
      .through(text.utf8Decode)
      .through(text.lines)
      .compile
      .toVector
      .map(_.mkString("\n"))
  }

  def write(path: Path, content: String, overwrite: Boolean = false): F[Unit] = {
    val outputStream: F[OutputStream] = F.delay(fs.create(path, overwrite))
    val sink = io.writeOutputStream(outputStream, blocker, closeAfterUse = true)

    Stream.emits(content.getBytes()).through(sink).compile.drain
  }

  def listDirectories(path: Path): F[List[Path]] = listStatus(path).map { fileStatuses =>
    fileStatuses
      .filter(_.isDirectory)
      .map(_.getPath)
  }

  def listFiles(path: Path): F[List[FileStatus]] = listStatus(path).map { fileStatuses =>
    fileStatuses
      .filter(!_.isDirectory)
  }

  def listStatus(path: Path): F[List[FileStatus]] = F.delay {
    fs.listStatus(path).toList
  }

}
