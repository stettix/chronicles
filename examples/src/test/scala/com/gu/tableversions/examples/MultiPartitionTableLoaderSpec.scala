package com.gu.tableversions.examples

import java.nio.file.Path
import java.sql.{Date, Timestamp}

import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.{UpdateMessage, UserId}
import com.gu.tableversions.core._
import com.gu.tableversions.spark.SparkHiveSuite
import com.gu.tableversions.spark.filesystem.VersionedFileSystem
import org.scalatest.{FlatSpec, Matchers}

/**
  * This example contains code that writes example event data to a table with multiple partition columns.
  * It demonstrates how individual partitions in such a table can be updated using versioning.
  */
class MultiPartitionTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  override def customConfig = VersionedFileSystem.sparkConfig("file", tableDir.toUri)

  import MultiPartitionTableLoaderSpec._

  "Writing multiple versions of a dataset with multiple partition columns" should "produce distinct partition versions" in {

    import spark.implicits._

    val versionContext = TestVersionContext.default.unsafeRunSync()
    import versionContext.metastore

    val table = TableDefinition(
      TableName(schema, "ad_impressions"),
      tableUri,
      PartitionSchema(List(PartitionColumn("impression_date"), PartitionColumn("processed_date"))),
      FileFormat.Orc
    )

    val ddl = s"""CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name.fullyQualifiedName} (
                 |  `user_id` string,
                 |  `ad_id` string,
                 |  `timestamp` timestamp
                 |)
                 |PARTITIONED BY (`impression_date` date, `processed_date` date)
                 |STORED AS orc
                 |LOCATION '${table.location}'
    """.stripMargin

    val loader = new TableLoader[AdImpression](versionContext, table, ddl, isSnapshot = false)
    val userId1 = UserId("test user 1")

    loader.initTable(userId1, UpdateMessage("init"))

    val impressionsDay1 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-13 23:59:00"), Date.valueOf("2019-03-14")),
      AdImpression("user-2", "ad-1", Timestamp.valueOf("2019-03-14 00:00:10"), Date.valueOf("2019-03-14")),
      AdImpression("user-3", "ad-2", Timestamp.valueOf("2019-03-14 00:00:20"), Date.valueOf("2019-03-14"))
    )

    loader.insert(impressionsDay1.toDS(), userId1, "Day 1 initial commit")
    loader.data().collect() should contain theSameElementsAs impressionsDay1

    val initialPartitionVersions = partitionVersions(tableDir)
    initialPartitionVersions("impression_date=2019-03-13" -> "processed_date=2019-03-14") should have size 1
    initialPartitionVersions("impression_date=2019-03-14" -> "processed_date=2019-03-14") should have size 1

    val impressionsDay2 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-14 23:59:00"), Date.valueOf("2019-03-15")),
      AdImpression("user-4", "ad-3", Timestamp.valueOf("2019-03-15 00:00:10"), Date.valueOf("2019-03-15"))
    )
    val userId2 = UserId("test user 2")

    loader.insert(impressionsDay2.toDS(), userId2, "Day 2 initial commit")
    loader.data().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2

    val partitionVersionsDay2 = partitionVersions(tableDir)
    partitionVersionsDay2(("impression_date=2019-03-13", "processed_date=2019-03-14")) shouldBe initialPartitionVersions(
      "impression_date=2019-03-13" -> "processed_date=2019-03-14")
    partitionVersionsDay2(("impression_date=2019-03-14", "processed_date=2019-03-14")) shouldBe initialPartitionVersions(
      "impression_date=2019-03-14" -> "processed_date=2019-03-14")
    partitionVersionsDay2("impression_date=2019-03-14" -> "processed_date=2019-03-15") should have size 1
    partitionVersionsDay2("impression_date=2019-03-15" -> "processed_date=2019-03-15") should have size 1

    // Rewrite impressions to change the content for one of the event dates
    val impressionsDay2Updated = impressionsDay2 ++ List(
      AdImpression("user-5", "ad-3", Timestamp.valueOf("2019-03-15 00:01:00"), Date.valueOf("2019-03-15"))
    )
    loader.insert(impressionsDay2Updated.toDS(), userId1, "Day 2 update")

    // Query to check we see the updated version
    loader.data().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2Updated

    // Check on-disk storage
    val updatedPartitionVersionsDay = partitionVersions(tableDir)
    updatedPartitionVersionsDay(("impression_date=2019-03-13", "processed_date=2019-03-14")) shouldBe initialPartitionVersions(
      "impression_date=2019-03-13" -> "processed_date=2019-03-14")
    updatedPartitionVersionsDay(("impression_date=2019-03-14", "processed_date=2019-03-14")) shouldBe initialPartitionVersions(
      "impression_date=2019-03-14" -> "processed_date=2019-03-14")
    updatedPartitionVersionsDay("impression_date=2019-03-14" -> "processed_date=2019-03-15") should have size 2
    updatedPartitionVersionsDay("impression_date=2019-03-14" -> "processed_date=2019-03-15") should contain allElementsOf partitionVersionsDay2(
      "impression_date=2019-03-14" -> "processed_date=2019-03-15")
    updatedPartitionVersionsDay("impression_date=2019-03-15" -> "processed_date=2019-03-15") should have size 2
    updatedPartitionVersionsDay("impression_date=2019-03-14" -> "processed_date=2019-03-15") should contain allElementsOf partitionVersionsDay2(
      "impression_date=2019-03-14" -> "processed_date=2019-03-15")

    // Get version history
    val versionHistory = metastore.updates(table.name).unsafeRunSync()
    versionHistory.size shouldBe 4 // One initial version plus three written versions

    // Roll back to previous version
    metastore.checkout(table.name, versionHistory.drop(1).head.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2

    // Roll forward to latest
    metastore.checkout(table.name, versionHistory.head.id).unsafeRunSync()
    loader.data().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2Updated
  }

  def partitionVersions(tableLocation: Path): Map[(String, String), List[String]] = {

    def datePartitions(columnName: String, dir: Path): List[String] = {
      val datePartitionPattern = s"$columnName=\\d\\d\\d\\d-\\d\\d-\\d\\d"
      dir.toFile.list().toList.filter(_.matches(datePartitionPattern))
    }

    def versions(dir: Path): List[String] =
      dir.toFile.list().toList.filter(_.matches(Version.TimestampAndUuidRegex.regex))

    val impressionDatePartitions: List[String] = datePartitions("impression_date", tableLocation)

    val allPartitions: List[(String, String)] = impressionDatePartitions.flatMap(impressionDate =>
      datePartitions("processed_date", tableLocation.resolve(impressionDate)).map(processedDate =>
        impressionDate -> processedDate))

    allPartitions
      .map {
        case (impressionDate, processedDate) =>
          (impressionDate, processedDate) -> versions(tableLocation.resolve(impressionDate).resolve(processedDate))
      }
      .toMap
      .filter { case (_, versions) => versions.nonEmpty }
  }

}

object MultiPartitionTableLoaderSpec {

  case class AdImpression(
      user_id: String,
      ad_id: String,
      timestamp: Timestamp,
      impression_date: Date,
      processed_date: Date)

  object AdImpression {

    def apply(userId: String, adId: String, timestamp: Timestamp, processedDate: Date): AdImpression =
      AdImpression(userId, adId, timestamp, DateTime.timestampToUtcDate(timestamp), processedDate)
  }

}
