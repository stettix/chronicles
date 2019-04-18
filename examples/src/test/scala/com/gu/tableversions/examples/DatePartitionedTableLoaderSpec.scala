package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths
import java.sql.Timestamp

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.spark.{SparkHiveMetastore, SparkHiveSuite}
import org.scalatest.{FlatSpec, Matchers}

class DatePartitionedTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import DatePartitionedTableLoader._

  val table = TableDefinition(
    TableName(schema, "pageview"),
    tableUri,
    PartitionSchema(List(PartitionColumn("date")))
  )

  "Writing multiple versions of a date partitioned dataset" should "produce distinct partition versions" in {

    import spark.implicits._
    implicit val tableVersions = InMemoryTableVersions[IO].unsafeRunSync()
    implicit val metastore = new SparkHiveMetastore[IO]()

    val loader =
      new DatePartitionedTableLoader(table)

    val userId = UserId("test user")
    loader.initTable(userId, UpdateMessage("init"))

    val pageviewsDay1 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-13 00:20:00")),
      Pageview("user-1", "news/uk", Timestamp.valueOf("2019-03-13 00:20:10")),
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-13 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-13 21:00:00"))
    )

    loader.insert(pageviewsDay1.toDS(), userId, "Day 1 initial commit")

    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1

    val pageviewsDay2 = List(
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-14 13:00:00")),
      Pageview("user-3", "sport/handball", Timestamp.valueOf("2019-03-14 21:00:00")),
      Pageview("user-4", "business", Timestamp.valueOf("2019-03-14 14:00:00"))
    )

    loader.insert(pageviewsDay2.toDS(), userId, "Day 2 initial commit")
    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2

    val pageviewsDay3 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-15 00:20:00")),
      Pageview("user-2", "news/politics/budget", Timestamp.valueOf("2019-03-15 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-15 21:00:00"))
    )

    loader.insert(pageviewsDay3.toDS(), userId, "Day 3 initial commit")

    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2 ++ pageviewsDay3

    // Check that data was written to the right partitions
    loader
      .pageviews()
      .where('date === "2019-03-13")
      .collect() should contain theSameElementsAs pageviewsDay1

    loader
      .pageviews()
      .where('date === "2019-03-14")
      .collect() should contain theSameElementsAs pageviewsDay2

    loader
      .pageviews()
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
    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3

    // Check underlying storage that we have both versions for the updated partition
    versionDirs(tableUri, "date=2019-03-13") should contain theSameElementsAs versionDirsFor13th
    val updatedVersionDirsFor14th = versionDirs(tableUri, "date=2019-03-14")
    updatedVersionDirsFor14th should have size 2
    updatedVersionDirsFor14th should contain allElementsOf versionDirsFor14th
    versionDirs(tableUri, "date=2019-03-15") should contain theSameElementsAs versionDirsFor15th

    // TODO: Query Metastore directly to check what partitions the table has?
    //   (When implementing rollback and creating views on historical versions we could just test that functionality
    //    instead of querying storage directly)
    //   Also: query version history for table

    // Check that we still have the previous version of the updated partition
    spark.read
      .parquet(tableUri.toString + s"/date=2019-03-14/${versionDirsFor14th.head}")
      .as[Pageview]
      .collect() should contain theSameElementsAs pageviewsDay2
  }

  def versionDirs(tableLocation: URI, partition: String): List[String] = {
    assert(tableLocation.toString.startsWith("file://"))
    val basePath = tableLocation.toString.drop("file://".length)
    val dir = Paths.get(s"$basePath/$partition")
    val dirList = Option(dir.toFile.list()).map(_.toList).getOrElse(Nil)
    dirList.filter(_.matches(Version.TimestampAndUuidRegex.regex))
  }

}
