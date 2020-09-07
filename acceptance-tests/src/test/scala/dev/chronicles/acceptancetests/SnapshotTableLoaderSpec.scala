package dev.chronicles.acceptancetests

import java.net.URI
import java.nio.file.Paths
import java.time.Instant

import dev.chronicles.core.VersionTracker.{UpdateMessage, UserId}
import dev.chronicles.core._
import dev.chronicles.hadoop.filesystem.VersionedFileSystem
import dev.chronicles.spark.{SparkHiveSuite, SparkSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * This tests the behaviour of an unpartitioned table, i.e. a table where we replace all the content
  * every time we write to it (no partial updates).
  */
class SnapshotTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  override def customConfig = VersionedFileSystem.sparkConfig("file", tableDir.toUri)

  import SnapshotTableLoaderSpec._

  val table = TableDefinition(TableName(schema, "users"), tableUri, PartitionSchema.snapshot, FileFormat.Parquet)

  val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
               |  `id` string,
               |  `name` string,
               |  `email` string
               |)
               |STORED AS parquet
               |LOCATION '${table.location}'
    """.stripMargin

  "Writing multiple versions of a snapshot dataset" should "produce distinct versions" in {
    import spark.implicits._

    val versionContext = TestVersionContext.default(spark).unsafeRunSync()
    val sparkSupport = SparkSupport(versionContext)
    import sparkSupport.syntax._

    val userId = UserId("test user")

    def tableData = spark.table(table.name.fullyQualifiedName).as[User]

    versionContext.metastore.tables().compile.toList.unsafeRunSync() shouldBe Nil

    // Create underlying table
    spark.sql(ddl)

    // Initialise version tracking for table
    versionContext.metastore
      .initTable(table.name, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())
      .unsafeRunSync()

    // Write the data to the table
    val identitiesDay1 = List(
      User("user-1", "Alice", "alice@mail.com"),
      User("user-2", "Bob", "bob@mail.com"),
      User("user-3", "Carol", "carol@mail.com")
    )
    identitiesDay1.toDS().versionedInsertInto(table, userId, "Committing first version from test")

    // We should see a new table
    versionContext.metastore.tables().compile.toList.unsafeRunSync() shouldBe List(table.name)

    // Query the table to make sure we have the right data
    val day1TableData = tableData.collect()
    day1TableData should contain theSameElementsAs identitiesDay1

    // Check underlying storage that the expected versions were written in the right place
    val initialTableVersionDirs = versionDirs(tableUri)
    initialTableVersionDirs should have size 1

    // Write a slightly changed version of the table
    val identitiesDay2 = List(
      User("user-2", "Bob", "bob@mail.com"),
      User("user-3", "Carol", "carol@othermail.com"),
      User("user-4", "Dave", "dave@mail.com")
    )
    identitiesDay2.toDS().versionedInsertInto(table, userId, "Committing second version from test")

    // We should still see a single table
    versionContext.metastore.tables().compile.toList.unsafeRunSync() shouldBe List(table.name)

    // Query it to make sure we have the right data
    val day2TableData = tableData.collect()
    day2TableData should contain theSameElementsAs identitiesDay2

    // Check underlying storage that it was written in the right place
    val updatedVersionDirs = versionDirs(tableUri)
    updatedVersionDirs should have size 2
    updatedVersionDirs should contain allElementsOf initialTableVersionDirs

    // Get version history
    val versionTracker = versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync()
    versionTracker.size shouldBe 3 // One initial version plus two written versions

    // Roll back to previous version
    versionContext.metastore.checkout(table.name, versionTracker.drop(1).head.id).unsafeRunSync()
    tableData.collect() should contain theSameElementsAs identitiesDay1

    // Roll forward to latest
    versionContext.metastore.checkout(table.name, versionTracker.head.id).unsafeRunSync()
    tableData.collect() should contain theSameElementsAs identitiesDay2
  }

  def versionDirs(tableLocation: URI): List[String] = {
    assert(tableLocation.toString.startsWith("file://"))
    val basePath = tableLocation.toString.drop("file://".length)
    val dir = Paths.get(basePath)
    dir.toFile.list().toList.filter(_.matches(Version.TimestampAndUuidRegex.regex))
  }

}

object SnapshotTableLoaderSpec {

  case class User(id: String, name: String, email: String)

}
