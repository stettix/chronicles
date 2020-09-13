package dev.chronicles.spark

import java.net.URI
import java.sql.Date
import java.time.Instant

import cats.effect.IO
import dev.chronicles.core.Metastore.TableChanges
import dev.chronicles.core.Metastore.TableOperation.{AddPartition, UpdateTableVersion}
import dev.chronicles.core.Partition.PartitionColumn
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core._
import dev.chronicles.spark.VersionContextSpec.{Event, User}
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class VersionContextSpec extends FlatSpec with Matchers with SparkHiveSuite {

  override def customConfig = Map("spark.sql.sources.partitionOverwriteMode" -> "dynamic")

  import spark.implicits._

  // Stub version generator that returns the same version on every invocation
  lazy val version1 = Version.generateVersion[IO].unsafeRunSync()
  lazy val version2 = Version.generateVersion[IO].unsafeRunSync()
  val generateVersion: IO[Version] = IO.pure(version1)

  "Inserting a snapshot dataset" should "write the data to the versioned location and commit the new version" in {
    val usersTable = TableDefinition(TableName(schema, "users"), tableUri, PartitionSchema.snapshot, FileFormat.Orc)

    val initialTableVersion = SnapshotTableVersion(Version.Unversioned)
    val stubbedChanges = TableChanges(List(UpdateTableVersion(version1)))

    val versionContext = {
      val stubMetastore: Metastore[IO] = new StubMetastore(
        currentVersion = initialTableVersion,
        computedChanges = stubbedChanges
      )

      val versionTracker: VersionTracker[IO] = (for {
        t <- InMemoryVersionTracker[IO]
        _ <- t.initTable(usersTable.name, isSnapshot = true, UserId("test"), UpdateMessage("init"), Instant.now())
      } yield t).unsafeRunSync()

      VersionContext(new VersionedMetastore(versionTracker, stubMetastore), generateVersion)
    }

    import versionContext._
    val ss = SparkSupport(versionContext)
    import ss.syntax._

    val users = List(
      User("101", "Alice"),
      User("102", "Bob")
    )

    val timestampBeforeWriting = Instant.now()
    val userId = UserId("test-user-id")

    // The insert method that we're testing here
    val (tableVersion, metastoreChanges) =
      users.toDS().versionedInsertInto(usersTable, userId, "Test insert users into table")

    // Check that data was written correctly to the right place
    readDataset[User](FileFormat.Orc, resolveTablePath(VersionPaths.VersionColumn + "=" + version1.label))
      .collect() should contain theSameElementsAs users

    // Check that we performed the right version updates and returned the right results
    tableVersion shouldBe SnapshotTableVersion(version1)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = metastore.updates(usersTable.name).compile.toList.unsafeRunSync()
    tableUpdates should have size 2
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert users into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  "Inserting a partitioned dataset" should "write the data to the versioned partitions and commit the new versions" in {
    val eventsTable =
      TableDefinition(TableName(schema, "events"),
                      tableUri,
                      PartitionSchema(List(PartitionColumn("date"))),
                      FileFormat.Parquet)

    val initialTableVersion = PartitionedTableVersion(partitionVersions = Map.empty)
    val stubbedChanges = TableChanges(initialTableVersion.partitionVersions.map(AddPartition.tupled).toList)

    val versionContext = {
      val stubMetastore: Metastore[IO] = new StubMetastore(
        currentVersion = initialTableVersion,
        computedChanges = stubbedChanges
      )

      val versionTracker: VersionTracker[IO] = (for {
        t <- InMemoryVersionTracker[IO]
        _ <- t.initTable(eventsTable.name, isSnapshot = false, UserId("test"), UpdateMessage("init"), Instant.now())
      } yield t).unsafeRunSync()

      VersionContext(new VersionedMetastore(versionTracker, stubMetastore), generateVersion)
    }

    import versionContext._
    val ss = SparkSupport(versionContext)
    import ss.syntax._

    val eventsDay1 = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15"))
    )
    val eventsDay2 = List(
      Event("103", "A", Date.valueOf("2019-01-16"))
    )
    val eventsDay3 = List(
      Event("104", "B", Date.valueOf("2019-01-17"))
    )
    val events = eventsDay1 ++ eventsDay2 ++ eventsDay3

    val timestampBeforeWriting = Instant.now()

    // Do the insert
    val userId = UserId("user-id")
    val (tableVersion, metastoreChanges) =
      events.toDS().versionedInsertInto(eventsTable, userId, "Test insert events into table")

    // Check that data was written correctly to the right place
    readDataset[Event](FileFormat.Parquet, tableUri)
      .collect() should contain theSameElementsAs (eventsDay1 ++ eventsDay2 ++ eventsDay3)

    // Check that we performed the right version updates and returned the right results
    val expectedPartitionVersions = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> version1,
      Partition(PartitionColumn("date"), "2019-01-16") -> version1,
      Partition(PartitionColumn("date"), "2019-01-17") -> version1
    )
    tableVersion shouldBe PartitionedTableVersion(expectedPartitionVersions)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = metastore.updates(eventsTable.name).compile.toList.unsafeRunSync()
    tableUpdates should have size 2
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert events into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  private def readDataset[T <: Product: TypeTag](fileFormat: FileFormat, path: URI): Dataset[T] =
    spark.read
      .format(fileFormat.name)
      .load(path.toString)
      .as[T]

  class StubMetastore(currentVersion: TableVersion, computedChanges: TableChanges) extends Metastore[IO] {

    override def currentVersion(table: TableName): IO[TableVersion] =
      IO(currentVersion)

    override def computeChanges(current: TableVersion, target: TableVersion): TableChanges =
      computedChanges

    override def update(table: TableName, changes: Metastore.TableChanges): IO[Unit] =
      IO.unit

  }

}

object VersionContextSpec {

  case class User(id: String, name: String)

  case class Event(id: String, value: String, date: Date)

}
