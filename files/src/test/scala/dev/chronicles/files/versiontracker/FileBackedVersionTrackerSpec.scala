package dev.chronicles.files.versiontracker

import java.net.URI
import java.nio.file.Files
import java.time.Instant

import cats.effect.{Blocker, ContextShift, IO}
import dev.chronicles.core.{TableName, VersionTrackerSpec}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks.{Table => TestTable, _}

import scala.util.Random
import FileBackedVersionTracker._

import scala.concurrent.ExecutionContext

class FileBackedVersionTrackerSpec extends FlatSpec with VersionTrackerSpec with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  val blocker: Blocker = Blocker.liftExecutionContext(ExecutionContext.global)

  val createVersionTracker: IO[FileBackedVersionTracker[IO]] =
    for {
      root <- IO(Files.createTempDirectory(getClass.getSimpleName))
      fs <- IO {
        val fs = new LocalFileSystem()
        fs.initialize(new URI("file:///"), new Configuration())
        fs
      }
      clock <- MonotonicClock[IO]
      fileSystem = PureFileSystem[IO](fs, blocker)
      versionTracker <- IO(new FileBackedVersionTracker[IO](fileSystem, new Path(root.toUri), clock))
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

  "The filename generated for a table update file" should "contain the timestamp in the correct format" in {
    val timestamp = Instant.parse("2021-12-03T10:15:30.01Z")
    FileBackedVersionTracker.tableUpdateFilename(timestamp) shouldBe "table_update_2021-12-03T10-15-30.010"
  }

  it should "be sortable in a way that's consistent with the associated timestamp" in {
    val r = new Random()
    val startTime = System.currentTimeMillis()
    val timestamps = (1 to 100).map(_ => startTime + r.nextInt(1000 * 60 * 60 * 24 * 100)).map(Instant.ofEpochMilli)

    val timestampsAndFilenames =
      timestamps.map(timestamp => timestamp -> FileBackedVersionTracker.tableUpdateFilename(timestamp))

    val sortedByTimestamp = timestampsAndFilenames.sortBy(_._1)
    val sortedByFilename = timestampsAndFilenames.sortBy(_._2)

    sortedByFilename should contain theSameElementsInOrderAs sortedByTimestamp
  }

}
