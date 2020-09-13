package dev.chronicles.acceptancetests

import java.net.URI
import java.nio.file.Paths
import java.sql.{Date, Timestamp}
import java.time.Instant

import dev.chronicles.core.Partition.PartitionColumn
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core._
import dev.chronicles.hadoop.filesystem.VersionedFileSystem
import dev.chronicles.spark.{SparkHiveSuite, SparkSupport}
import org.scalatest.{FlatSpec, Matchers}

/**
  * This tests the behaviour of a table partitioned by a single column, where partitions are versioned individually.
  */
class DatePartitionedTableSpec extends FlatSpec with Matchers with SparkHiveSuite {

  override def customConfig = VersionedFileSystem.sparkConfig("file", tableDir.toUri)

  import DatePartitionedTableSpec._

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
    val sparkSupport = SparkSupport(versionContext)
    import sparkSupport.syntax._

    val userId = UserId("test user")

    // Create underlying table
    spark.sql(ddl)

    // Initialise version tracking for table
    versionContext.metastore
      .initTable(table.name, isSnapshot = false, userId, UpdateMessage("init"), Instant.now())
      .unsafeRunSync()
    versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync() should have size 1

    val pageviewsDay1 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-13 00:20:00")),
      Pageview("user-1", "news/uk", Timestamp.valueOf("2019-03-13 00:20:10")),
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-13 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-13 21:00:00"))
    )

    pageviewsDay1.toDS().versionedInsertInto(table, userId, "Day 1 initial commit")

    def tableData = spark.table(table.name.fullyQualifiedName).as[Pageview]

    tableData.collect() should contain theSameElementsAs pageviewsDay1

    versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync() should have size 2

    val pageviewsDay2 = List(
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-14 13:00:00")),
      Pageview("user-3", "sport/handball", Timestamp.valueOf("2019-03-14 21:00:00")),
      Pageview("user-4", "business", Timestamp.valueOf("2019-03-14 14:00:00"))
    )

    pageviewsDay2.toDS().versionedInsertInto(table, userId, "Day 2 initial commit")
    tableData.collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2
    versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync() should have size 3

    val pageviewsDay3 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-15 00:20:00")),
      Pageview("user-2", "news/politics/budget", Timestamp.valueOf("2019-03-15 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-15 21:00:00"))
    )

    pageviewsDay3.toDS().versionedInsertInto(table, userId, "Day 3 initial commit")
    tableData.collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3
    versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync() should have size 4

    // Check that data was written to the right partitions
    tableData
      .where('date === "2019-03-13")
      .collect() should contain theSameElementsAs pageviewsDay1

    tableData
      .where('date === "2019-03-14")
      .collect() should contain theSameElementsAs pageviewsDay2

    tableData
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
    updatedPageviewsDay2.toDS().versionedInsertInto(table, UserId("another user"), "Reprocess day 2")

    // Query to check we see the updated data
    tableData.collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3

    // Check underlying storage that we have both versions for the updated partition
    versionDirs(tableUri, "date=2019-03-13") should contain theSameElementsAs versionDirsFor13th
    val updatedVersionDirsFor14th = versionDirs(tableUri, "date=2019-03-14")
    updatedVersionDirsFor14th should have size 2
    updatedVersionDirsFor14th should contain allElementsOf versionDirsFor14th
    versionDirs(tableUri, "date=2019-03-15") should contain theSameElementsAs versionDirsFor15th

    // Roll back to a previous version and check see the data of the old version
    val versionHistory = versionContext.metastore.updates(table.name).compile.toList.unsafeRunSync()
    val previousVersion = versionHistory.drop(1).head
    versionContext.metastore.checkout(table.name, previousVersion.id).unsafeRunSync()
    tableData.collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3

    // Roll back to an even earlier version
    versionContext.metastore.checkout(table.name, versionHistory.takeRight(2).head.id).unsafeRunSync()
    tableData.collect() should contain theSameElementsAs pageviewsDay1

    // Roll back to the table as it looked after init, before any partitions were added.
    versionContext.metastore.checkout(table.name, versionHistory.last.id).unsafeRunSync()
    tableData.collect() shouldBe empty

    // Write some new data to the table
    // Check that we're on the very latest version after this, checking it automatically moves to the head version.
    val pageviewsDay4 = List(
      Pageview("user-99", "news/technology/surveillance", Timestamp.valueOf("2019-03-16 10:20:30"))
    )
    pageviewsDay4.toDS().versionedInsertInto(table, userId, "Day 4 initial commit")

    tableData
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

object DatePartitionedTableSpec {
  case class Pageview(id: String, path: String, timestamp: Timestamp, date: Date)

  object Pageview {

    def apply(id: String, path: String, timestamp: Timestamp): Pageview =
      Pageview(id, path, timestamp, DateTime.timestampToUtcDate(timestamp))

  }

}
