package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths

import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.spark.SparkHiveSuite
import com.gu.tableversions.spark.filesystem.VersionedFileSystem
import org.scalatest.{FlatSpec, Matchers}

/**
  * This is an example of loading data into a 'snapshot' table, that is, a table where we replace all the content
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

    val versionContext = TestVersionContext.default.unsafeRunSync()
    import versionContext.metastore

    val userId = UserId("test user")

    val loader = new TableLoader[User](versionContext, table, ddl, isSnapshot = true)
    loader.initTable(userId, UpdateMessage("init"))

    // Write the data to the table
    val identitiesDay1 = List(
      User("user-1", "Alice", "alice@mail.com"),
      User("user-2", "Bob", "bob@mail.com"),
      User("user-3", "Carol", "carol@mail.com")
    )
    loader.insert(identitiesDay1.toDS(), userId, "Committing first version from test")

    // Query the table to make sure we have the right data
    val day1TableData = loader.data().collect()
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
    loader.insert(identitiesDay2.toDS(), userId, "Committing second version from test")

    // Query it to make sure we have the right data
    val day2TableData = loader.data().collect()
    day2TableData should contain theSameElementsAs identitiesDay2

    // Check underlying storage that it was written in the right place
    val updatedVersionDirs = versionDirs(tableUri)
    updatedVersionDirs should have size 2
    updatedVersionDirs should contain allElementsOf initialTableVersionDirs

    // Get version history
    val versionHistory = metastore.updates(table.name).unsafeRunSync()
    versionHistory.size shouldBe 3 // One initial version plus two written versions

    // Roll back to previous version
    metastore.checkout(table.name, versionHistory.drop(1).head.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs identitiesDay1

    // Roll forward to latest
    metastore.checkout(table.name, versionHistory.head.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs identitiesDay2
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
