package dev.chronicles.files.versiontracker

import java.net.URI
import java.nio.file.Files
import java.nio.file.{Path => JPath}

import cats.effect.IO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class FileSystemSyntaxSpec extends FlatSpec with Matchers {

  // Create a temporary directory and the Filesystem objects under test.
  val init: IO[(JPath, FileSystem, FileSystemSyntax[IO])] = for {
    root <- IO(Files.createTempDirectory(getClass.getSimpleName))
    fs <- IO {
      val fs = new LocalFileSystem()
      fs.initialize(new URI("file:///"), new Configuration())
      fs
    }
    fsSyntax <- IO((root, fs, FileSystemSyntax[IO]()))
  } yield fsSyntax

  "Performing filesystem operations" should "work as expected" in {

    val test = init.flatMap {
      case (root, fs, fsSyntax) =>
        import fsSyntax._

        val rootPath = new Path(root.toUri)
        val dir1 = new Path(rootPath, "dir1")
        val dir2 = new Path(rootPath, "dir2")

        for {
          subDirsBeforeCreating <- fs.listDirectories(rootPath)
          _ <- IO(assert(subDirsBeforeCreating == Nil))

          _ <- fs.createDirectory(dir1)
          _ <- fs.createDirectory(dir2)
          _ <- fs.write(new Path(rootPath, "fileInRoot"), "This file is in the root directory")
          _ <- fs.write(new Path(dir1, "fileInDir1"), "This file is in dir1")

          subDirsAfterCreating <- fs.listDirectories(rootPath)
          _ <- IO(assert(subDirsAfterCreating.toSet == Set(dir1, dir2)))

          dir1Exists <- fs.directoryExists(dir1)
          _ <- IO(assert(dir1Exists))

          otherDirExists <- fs.directoryExists(new Path(rootPath, "foobar"))
          _ <- IO(assert(!otherDirExists))

          fileInRootReadBack <- fs.readString(new Path(rootPath, "fileInRoot"))
          _ <- IO(assert(fileInRootReadBack == "This file is in the root directory"))

          fileInDir1ReadBack <- fs.readString(new Path(dir1, "fileInDir1"))
          _ <- IO(assert(fileInDir1ReadBack == "This file is in dir1"))

        } yield ()
    }

    test.unsafeRunSync()
  }

}
