package com.gu.tableversions.spark

import java.net.URI
import java.sql.Date
import java.time.Instant

import cats.effect.IO
import cats.effect.concurrent.Ref
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.{CommitResult, UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.Metastore.TableOperation.AddPartition
import com.gu.tableversions.spark.VersionedDatasetSpec.{Event, User}
import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.runtime.universe.TypeTag

class VersionedDatasetSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import spark.implicits._

  "Finding the partitions of a dataset" should "return the empty partition for an un-partitioned dataset" in {

    val snapshotDataset: Dataset[User] = List(
      User("101", "Alice"),
      User("102", "Bob")
    ).toDS()

    val schema = PartitionSchema.snapshot

    VersionedDataset.partitionValues(snapshotDataset, schema) shouldBe List(Partition.snapshotPartition)
  }

  it should "return all partitions for a dataset with a single partition column" in {

    val partitionedDataset: Dataset[Event] = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    ).toDS()

    val schema = PartitionSchema(List(PartitionColumn("date")))

    val expectedPartitions = List(
      Partition(PartitionColumn("date"), "2019-01-15"),
      Partition(PartitionColumn("date"), "2019-01-16"),
      Partition(PartitionColumn("date"), "2019-01-18")
    )
    VersionedDataset.partitionValues(partitionedDataset, schema) should contain theSameElementsAs expectedPartitions
  }

  it should "return no partitions for an empty dataset with a partitioned schema" in {
    val schema = PartitionSchema(List(PartitionColumn("date")))
    VersionedDataset.partitionValues(spark.emptyDataset[Event], schema) shouldBe empty
  }

  "Writing a dataset with multiple partitions" should "store the data for each partition in a versioned folder for the partition" in {

    val events = List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15")),
      Event("103", "A", Date.valueOf("2019-01-16")),
      Event("104", "B", Date.valueOf("2019-01-18"))
    ).toDS()

    val partitionPaths: Map[Partition, URI] = Map(
      Partition(PartitionColumn("date"), "2019-01-15") -> tableUri.resolve("date=2019-01-15/v5"),
      Partition(PartitionColumn("date"), "2019-01-16") -> tableUri.resolve("date=2019-01-16/v1"),
      Partition(PartitionColumn("date"), "2019-01-18") -> tableUri.resolve("date=2019-01-18/v3")
    )

    VersionedDataset.writeVersionedPartitions(events, partitionPaths)

    // Check that data was written to the right place.

    readDataset[Event](tableUri.resolve("date=2019-01-15/v5")).collect() should contain theSameElementsAs List(
      Event("101", "A", Date.valueOf("2019-01-15")),
      Event("102", "B", Date.valueOf("2019-01-15"))
    )

    readDataset[Event](tableUri.resolve("date=2019-01-16/v1")).collect() should contain theSameElementsAs List(
      Event("103", "A", Date.valueOf("2019-01-16"))
    )

    readDataset[Event](tableUri.resolve("date=2019-01-18/v3")).collect() should contain theSameElementsAs List(
      Event("104", "B", Date.valueOf("2019-01-18"))
    )
  }

  "Writing a snapshot dataset" should "store the data in a versioned folder for the whole table" in {
    val users = List(
      User("101", "Alice"),
      User("102", "Bob")
    )
    val partitionPaths = Map(Partition.snapshotPartition -> tableUri.resolve("v5"))

    VersionedDataset.writeVersionedPartitions(users.toDS(), partitionPaths)

    readDataset[User](tableUri.resolve("v5")).collect() should contain theSameElementsAs users
  }

  "Inserting a snapshot dataset" should "write the data to the versioned location and commit the new version" in {
    import VersionedDataset._

    val usersTable = TableDefinition(TableName(schema, "users"), tableUri, PartitionSchema.snapshot)

    // Stub metastore
    val initialTableVersion = TableVersion(partitionVersions = Nil)
    val nextPartitionVersions = List(PartitionVersion(Partition.snapshotPartition, VersionNumber(1)))

    val stubbedChanges = TableChanges(nextPartitionVersions.map(AddPartition))
    implicit val stubMetastore: Metastore[IO] = new StubMetastore(
      currentVersion = initialTableVersion,
      computedChanges = stubbedChanges
    )

    // Stub table versions
    val committedTableUpdatesRef = Ref[IO].of(List.empty[TableVersions.TableUpdate]).unsafeRunSync()
    implicit val stubTableVersions = new StubTableVersions(
      currentVersions = Map(usersTable.name -> TableVersion(nextPartitionVersions)),
      nextVersions = Map(usersTable.name -> nextPartitionVersions),
      committedTableUpdatesRef
    )

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
    readDataset[User](resolveTablePath("v1")).collect() should contain theSameElementsAs users

    // Check that we performed the right version updates and returned the right results
    tableVersion shouldBe TableVersion(nextPartitionVersions)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = stubTableVersions.committedTableUpdates.unsafeRunSync()
    tableUpdates should have size 1
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert users into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  "Inserting a partitioned dataset" should "write the data to the versioned partitions and commit the new versions" in {
    import VersionedDataset._

    val eventsTable = TableDefinition(TableName(schema, "events"), tableUri, PartitionSchema.snapshot)

    val initialTableVersion = TableVersion(partitionVersions = Nil)
    val nextPartitionVersions = List(
      PartitionVersion(Partition(PartitionColumn("date"), "2019-01-15"), VersionNumber(3)),
      PartitionVersion(Partition(PartitionColumn("date"), "2019-01-16"), VersionNumber(2)),
      PartitionVersion(Partition(PartitionColumn("date"), "2019-01-17"), VersionNumber(1))
    )
    val stubbedChanges = TableChanges(initialTableVersion.partitionVersions.map(AddPartition))

    // Stub metastore
    implicit val stubMetastore: Metastore[IO] = new StubMetastore(
      currentVersion = initialTableVersion,
      computedChanges = stubbedChanges
    )

    // Stub table versions
    val committedTableUpdatesRef = Ref[IO].of(List.empty[TableVersions.TableUpdate]).unsafeRunSync()
    implicit val stubTableVersions = new StubTableVersions(
      currentVersions = Map(eventsTable.name -> TableVersion(nextPartitionVersions)),
      nextVersions = Map(eventsTable.name -> nextPartitionVersions),
      committedTableUpdatesRef
    )

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
    readDataset[Event](resolveTablePath("date=2019-01-15/v3")).collect() should contain theSameElementsAs eventsDay1
    readDataset[Event](resolveTablePath("date=2019-01-16/v2")).collect() should contain theSameElementsAs eventsDay2
    readDataset[Event](resolveTablePath("date=2019-01-17/v1")).collect() should contain theSameElementsAs eventsDay3

    // Check that we performed the right version updates and returned the right results
    tableVersion shouldBe TableVersion(nextPartitionVersions)
    metastoreChanges shouldBe stubbedChanges

    val tableUpdates = stubTableVersions.committedTableUpdates.unsafeRunSync()
    tableUpdates should have size 1
    val tableUpdate = tableUpdates.head
    tableUpdate.message shouldBe UpdateMessage("Test insert events into table")
    timestampBeforeWriting.isAfter(tableUpdate.timestamp) shouldBe false
    tableUpdate.userId shouldBe userId
  }

  private def readDataset[T <: Product: TypeTag](path: URI): Dataset[T] =
    spark.read
      .parquet(path.toString)
      .as[T]

  class StubTableVersions(
      currentVersions: Map[TableName, TableVersion],
      nextVersions: Map[TableName, List[PartitionVersion]],
      committedTableUpdatesRef: Ref[IO, List[TableVersions.TableUpdate]])
      extends TableVersions[IO] {

    def committedTableUpdates: IO[List[TableVersions.TableUpdate]] =
      committedTableUpdatesRef.get

    override def init(table: TableName): IO[Unit] =
      IO.unit

    override def currentVersion(table: TableName): IO[TableVersion] =
      IO(currentVersions(table))

    override def nextVersions(table: TableName, partitions: List[Partition]): IO[List[PartitionVersion]] =
      IO(nextVersions(table))

    override def commit(newVersion: TableVersions.TableUpdate): IO[TableVersions.CommitResult] =
      committedTableUpdatesRef.update(_ :+ newVersion).map(_ => CommitResult.SuccessfulCommit)
  }

  class StubMetastore(currentVersion: TableVersion, computedChanges: TableChanges) extends Metastore[IO] {

    override def currentVersion(table: TableName): IO[Option[TableVersion]] =
      IO(Some(currentVersion))

    override def computeChanges(current: TableVersion, target: TableVersion): TableChanges =
      computedChanges

    override def update(table: TableName, changes: Metastore.TableChanges): IO[Unit] =
      IO.unit

  }

}

object VersionedDatasetSpec {

  case class User(id: String, name: String)

  case class Event(id: String, value: String, date: Date)

}
