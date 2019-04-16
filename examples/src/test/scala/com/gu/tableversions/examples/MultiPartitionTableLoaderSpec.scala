package com.gu.tableversions.examples

import java.nio.file.Path
import java.sql.{Date, Timestamp}

import cats.effect.IO
import com.gu.tableversions.core.Partition.{ColumnValue, PartitionColumn}
import com.gu.tableversions.core._
import com.gu.tableversions.core.TableVersions.UserId
import com.gu.tableversions.examples.MultiPartitionTableLoader.AdImpression
import com.gu.tableversions.spark.{SparkHiveMetastore, SparkHiveSuite}
import org.scalatest.{FlatSpec, Matchers}

class MultiPartitionTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  "Writing multiple versions of a dataset with multiple partition columns" should "produce distinct partition versions" in {

    import spark.implicits._
    implicit val tableVersions = InMemoryTableVersions[IO].unsafeRunSync()
    implicit val metastore = new SparkHiveMetastore[IO]()

    val table = TableDefinition(
      TableName(schema, "ad_impressions"),
      tableUri,
      PartitionSchema(List(PartitionColumn("impression_date"), PartitionColumn("processed_date")))
    )

    val loader = new MultiPartitionTableLoader(table)
    loader.initTable()

    val impressionsDay1 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-13 23:59:00"), Date.valueOf("2019-03-14")),
      AdImpression("user-2", "ad-1", Timestamp.valueOf("2019-03-14 00:00:10"), Date.valueOf("2019-03-14")),
      AdImpression("user-3", "ad-2", Timestamp.valueOf("2019-03-14 00:00:20"), Date.valueOf("2019-03-14"))
    )
    val userId1 = UserId("test user 1")

    loader.insert(impressionsDay1.toDS(), userId1, "Day 1 initial commit")
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1

    partitionVersions(tableDir) shouldBe Map(
      ("impression_date=2019-03-13", "processed_date=2019-03-14") -> List("v1"),
      ("impression_date=2019-03-14", "processed_date=2019-03-14") -> List("v1")
    )

    val impressionsDay2 = List(
      AdImpression("user-1", "ad-1", Timestamp.valueOf("2019-03-14 23:59:00"), Date.valueOf("2019-03-15")),
      AdImpression("user-4", "ad-3", Timestamp.valueOf("2019-03-15 00:00:10"), Date.valueOf("2019-03-15"))
    )
    val userId2 = UserId("test user 2")

    loader.insert(impressionsDay2.toDS(), userId2, "Day 2 initial commit")
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2

    partitionVersions(tableDir) shouldBe Map(
      ("impression_date=2019-03-13", "processed_date=2019-03-14") -> List("v1"),
      ("impression_date=2019-03-14", "processed_date=2019-03-14") -> List("v1"),
      ("impression_date=2019-03-14", "processed_date=2019-03-15") -> List("v1"),
      ("impression_date=2019-03-15", "processed_date=2019-03-15") -> List("v1")
    )

    // Rewrite impressions to change the content for one of the event dates
    val impressionsDay2Updated = impressionsDay2 ++ List(
      AdImpression("user-5", "ad-3", Timestamp.valueOf("2019-03-15 00:01:00"), Date.valueOf("2019-03-15"))
    )
    loader.insert(impressionsDay2Updated.toDS(), userId1, "Day 2 update")

    // Metastore should point to new partition versions.
    // Note that the unchanged partition that was rewritten has a new version as well, there is no diffing of data
    // to try to avoid this.
    val tableVersionAfterUpdate = metastore.currentVersion(table.name).unsafeRunSync()
    tableVersionAfterUpdate shouldBe
      TableVersion(
        List(
          PartitionVersion(
            Partition(List(ColumnValue(PartitionColumn("impression_date"), "2019-03-13"),
                           ColumnValue(PartitionColumn("processed_date"), "2019-03-14"))),
            VersionNumber(1)
          ),
          PartitionVersion(
            Partition(List(ColumnValue(PartitionColumn("impression_date"), "2019-03-14"),
                           ColumnValue(PartitionColumn("processed_date"), "2019-03-14"))),
            VersionNumber(1)
          ),
          PartitionVersion(
            Partition(List(ColumnValue(PartitionColumn("impression_date"), "2019-03-14"),
                           ColumnValue(PartitionColumn("processed_date"), "2019-03-15"))),
            VersionNumber(2)
          ),
          PartitionVersion(
            Partition(List(ColumnValue(PartitionColumn("impression_date"), "2019-03-15"),
                           ColumnValue(PartitionColumn("processed_date"), "2019-03-15"))),
            VersionNumber(2)
          )
        ))

    // Query to check we see the updated version
    loader.adImpressions().collect() should contain theSameElementsAs impressionsDay1 ++ impressionsDay2Updated

    // Check on-disk storage
    partitionVersions(tableDir) shouldBe Map(
      ("impression_date=2019-03-13", "processed_date=2019-03-14") -> List("v1"),
      ("impression_date=2019-03-14", "processed_date=2019-03-14") -> List("v1"),
      ("impression_date=2019-03-14", "processed_date=2019-03-15") -> List("v1", "v2"),
      ("impression_date=2019-03-15", "processed_date=2019-03-15") -> List("v1", "v2")
    )

  }

  def partitionVersions(tableLocation: Path): Map[(String, String), List[String]] = {

    def datePartitions(columnName: String, dir: Path): List[String] = {
      val datePartitionPattern = s"$columnName=\\d\\d\\d\\d-\\d\\d-\\d\\d"
      dir.toFile.list().toList.filter(_.matches(datePartitionPattern))
    }

    def versions(dir: Path): List[String] =
      dir.toFile.list().toList.filter(_.matches("v\\d+"))

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
