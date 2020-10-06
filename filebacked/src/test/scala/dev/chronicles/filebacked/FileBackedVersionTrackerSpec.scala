package dev.chronicles.filebacked

import java.net.URI
import java.nio.file.Files

import cats.effect.IO
import dev.chronicles.core.{TableName, VersionTrackerSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks.{Table => TestTable, _}

class FileBackedVersionTrackerSpec extends FlatSpec with VersionTrackerSpec with Matchers {

  import FileBackedVersionTracker.TableDirectoryPrefix

  val createVersionTracker: IO[FileBackedVersionTracker[IO]] = for {
    root <- IO(Files.createTempDirectory(getClass.getSimpleName))
    fs <- IO {
      val fs = new LocalFileSystem()
      fs.initialize(new URI("file:///"), new Configuration())
      fs
    }
    versionTracker <- IO(new FileBackedVersionTracker[IO](fs, new Path(root.toUri)))
  } yield versionTracker

  "The file backed implementation for the service" should behave like versionTrackerBehaviour {
    createVersionTracker
  }

  "Parsing table folder names" should "return the table names for matching folders only" in {
    val folderNames = TestTable(
      ("folder name", "expected table name"),
      (s"${TableDirectoryPrefix}my_database.my_table", Some(TableName("my_database", "my_table"))),
      (s"${TableDirectoryPrefix}my_database.my_table", Some(TableName("my_database", "my_table"))),
      (s"${TableDirectoryPrefix}MY_DATABASE.MY_TABLE", Some(TableName("MY_DATABASE", "MY_TABLE"))),
      (s"s3:///foo/bar/some_random_path", None)
    )

    forAll(folderNames) { (folderName, expectedTableName) =>
      FileBackedVersionTracker.parseTableName(folderName) shouldBe expectedTableName
    }
  }

}
