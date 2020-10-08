package dev.chronicles.files.versiontracker

import java.net.URI
import java.nio.file.{Files, Path => JPath}

import cats.effect.{Blocker, ContextShift, IO}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class FileSystemSyntaxSpec extends FlatSpec with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  val blocker: Blocker = Blocker.liftExecutionContext(ExecutionContext.global)

  // Create a temporary directory and the Filesystem objects under test.
  val init: IO[(JPath, FileSystem, FileSystemSyntax[IO])] =
    for {
      root <- IO(Files.createTempDirectory(getClass.getSimpleName))
      fs <- IO {
        val fs = new LocalFileSystem()
        fs.initialize(new URI("file:///"), new Configuration())
        fs
      }
      fsSyntax <- IO((root, fs, FileSystemSyntax[IO](blocker)))
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
          _ <- fs.write(new Path(dir1, "anotherFileInDir1"), "This file is in dir1 too")

          subDirsAfterCreating <- fs.listDirectories(rootPath)
          _ <- IO(assert(subDirsAfterCreating.toSet == Set(dir1, dir2)))

          dir1Exists <- fs.directoryExists(dir1)
          _ <- IO(assert(dir1Exists))

          otherDirExists <- fs.directoryExists(new Path(rootPath, "foobar"))
          _ <- IO(assert(!otherDirExists))

          filesInDir1 <- fs.listFiles(dir1)
          _ <- IO(assert(filesInDir1.map(_.getPath.getName).toSet == Set("fileInDir1", "anotherFileInDir1")))

          fileInRootReadBack <- fs.readString(new Path(rootPath, "fileInRoot"))
          _ <- IO(assert(fileInRootReadBack == "This file is in the root directory"))

          fileInDir1ReadBack <- fs.readString(new Path(dir1, "fileInDir1"))
          _ <- IO(assert(fileInDir1ReadBack == "This file is in dir1"))

        } yield ()
    }

    test.unsafeRunSync()
  }

}
