package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths
import java.sql.{Date, Timestamp}

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions._
import com.gu.tableversions.core._
import com.gu.tableversions.spark.SparkHiveSuite
import com.gu.tableversions.spark.filesystem.VersionedFileSystem
import org.scalatest.{FlatSpec, Matchers}

/**
  * This example contains code that writes example event data to a table that has a single date partition.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  */
class DatePartitionedTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  override def customConfig = VersionedFileSystem.sparkConfig("file", tableDir.toUri)

  import DatePartitionedTableLoaderSpec._

  val table = TableDefinition(
    TableName(schema, "pageview"),
    tableUri,
    PartitionSchema(List(PartitionColumn("date"))),
    FileFormat.Parquet
  )

  val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
               |  `id` string,
               |  `path` string,
               |  `timestamp` timestamp
               |)
               |PARTITIONED BY (`date` date)
               |STORED AS parquet
               |LOCATION '${table.location}'
    """.stripMargin

  "Writing multiple versions of a date partitioned dataset" should "produce distinct partition versions" in {

    import spark.implicits._

    val versionContext = TestVersionContext.default.unsafeRunSync()
    import versionContext.metastore

    val loader = new TableLoader[Pageview](versionContext, table, ddl, isSnapshot = false)

    val userId = UserId("test user")
    loader.initTable(userId, UpdateMessage("init"))
    metastore.updates(table.name).unsafeRunSync() should have size 1

    val pageviewsDay1 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-13 00:20:00")),
      Pageview("user-1", "news/uk", Timestamp.valueOf("2019-03-13 00:20:10")),
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-13 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-13 21:00:00"))
    )

    loader.insert(pageviewsDay1.toDS(), userId, "Day 1 initial commit")

    loader.data().collect() should contain theSameElementsAs pageviewsDay1

    metastore.updates(table.name).unsafeRunSync() should have size 2

    val pageviewsDay2 = List(
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-14 13:00:00")),
      Pageview("user-3", "sport/handball", Timestamp.valueOf("2019-03-14 21:00:00")),
      Pageview("user-4", "business", Timestamp.valueOf("2019-03-14 14:00:00"))
    )

    loader.insert(pageviewsDay2.toDS(), userId, "Day 2 initial commit")
    loader.data().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2
    metastore.updates(table.name).unsafeRunSync() should have size 3

    val pageviewsDay3 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-15 00:20:00")),
      Pageview("user-2", "news/politics/budget", Timestamp.valueOf("2019-03-15 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-15 21:00:00"))
    )

    loader.insert(pageviewsDay3.toDS(), userId, "Day 3 initial commit")
    loader.data().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3
    metastore.updates(table.name).unsafeRunSync() should have size 4

    // Check that data was written to the right partitions
    loader
      .data()
      .where('date === "2019-03-13")
      .collect() should contain theSameElementsAs pageviewsDay1

    loader
      .data()
      .where('date === "2019-03-14")
      .collect() should contain theSameElementsAs pageviewsDay2

    loader
      .data()
      .where('date === "2019-03-15")
      .collect() should contain theSameElementsAs pageviewsDay3

    val versionDirsFor13th = versionDirs(tableUri, "date=2019-03-13")
    versionDirsFor13th should have size 1
    val versionDirsFor14th = versionDirs(tableUri, "date=2019-03-14")
    versionDirsFor14th should have size 1
    val versionDirsFor15th = versionDirs(tableUri, "date=2019-03-15")
    versionDirsFor15th should have size 1

    // Rewrite pageviews to remove one of the identity IDs, affecting only day 2
    // TODO: Change this to affect multiple partitions
    val updatedPageviewsDay2 = pageviewsDay2.filter(_.id != "user-4")
    loader.insert(updatedPageviewsDay2.toDS(), UserId("another user"), "Reprocess day 2")

    // Query to check we see the updated data
    loader.data().collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3

    // Check underlying storage that we have both versions for the updated partition
    versionDirs(tableUri, "date=2019-03-13") should contain theSameElementsAs versionDirsFor13th
    val updatedVersionDirsFor14th = versionDirs(tableUri, "date=2019-03-14")
    updatedVersionDirsFor14th should have size 2
    updatedVersionDirsFor14th should contain allElementsOf versionDirsFor14th
    versionDirs(tableUri, "date=2019-03-15") should contain theSameElementsAs versionDirsFor15th

    // Roll back to a previous version and check see the data of the old version
    val versionHistory = metastore.updates(table.name).unsafeRunSync()
    val previousVersion = versionHistory.drop(1).head
    metastore.checkout(table.name, previousVersion.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3

    // Roll back to an even earlier version
    metastore.checkout(table.name, versionHistory.takeRight(2).head.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs pageviewsDay1

    // Roll back to the table as it looked after init, before any partitions were added.
    metastore.checkout(table.name, versionHistory.last.id).unsafeRunSync()
    loader.data().collect() shouldBe empty

    // Write some new data to the table
    // Check that we're on the very latest version after this, checking it automatically moves to the head version.
    val pageviewsDay4 = List(
      Pageview("user-99", "news/technology/surveillance", Timestamp.valueOf("2019-03-16 10:20:30"))
    )
    loader.insert(pageviewsDay4.toDS(), userId, "Day 4 initial commit")

    loader
      .data()
      .collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3 ++ pageviewsDay4
  }

  def versionDirs(tableLocation: URI, partition: String): List[String] = {
    assert(tableLocation.toString.startsWith("file://"))
    val basePath = tableLocation.toString.drop("file://".length)
    val dir = Paths.get(s"$basePath/$partition")
    val dirList = Option(dir.toFile.list()).map(_.toList).getOrElse(Nil)
    dirList.filter(_.matches(Version.TimestampAndUuidRegex.regex))
  }

}

object DatePartitionedTableLoaderSpec {
  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, DateTime.timestampToUtcDate(timestamp))

  }

}
