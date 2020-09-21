package dev.chronicles.filebacked

import java.nio.charset.StandardCharsets

import cats.effect.Sync
import cats.implicits._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.{Codec, Source}

/**
  * This adds methods on a FileSystem object that use effect types hence allow files and directories
  * to be manpulated in a pure way.
  */
case class FileSystemSyntax[F[_]]()(implicit F: Sync[F]) {

  implicit class FileSystemMethods(fs: FileSystem) {

    def directoryExists(path: Path): F[Boolean] = F.delay(fs.exists(path))

    def createDirectory(path: Path): F[Unit] = F.delay(fs.mkdirs(path)).void

    def readString(path: Path): F[String] = F.delay {
      val source = Source.fromInputStream(fs.open(path))(Codec.UTF8)
      source.getLines().mkString("\n")
      // TODO: Do I need to close the Source here?
    }

    def write(path: Path, content: String): F[Unit] = F.delay {
      val outputStream = fs.create(path)
      try {
        outputStream.write(content.getBytes(StandardCharsets.UTF_8))
        outputStream.flush()
      } finally {
        outputStream.close()
      }
    }

    def listDirectories(path: Path): F[List[Path]] = F.delay {
      fs.listStatus(path)
        .filter(_.isDirectory)
        .map(_.getPath)
        .toList
    }

  }

}
