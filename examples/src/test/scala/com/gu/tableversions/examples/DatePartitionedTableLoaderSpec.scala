package com.gu.tableversions.examples

import java.net.URI
import java.nio.file.Paths
import java.sql.Timestamp

import com.gu.tableversions.spark.SparkHiveSuite
import org.scalatest.{FlatSpec, Matchers}

class DatePartitionedTableLoaderSpec extends FlatSpec with Matchers with SparkHiveSuite {

  import DatePartitionedTableLoader._

  "Writing multiple versions of a date partitioned dataset" should "produce distinct partiton versions" ignore {

    import spark.implicits._

    val loader = new DatePartitionedTableLoader(s"$schema.pageview", tableDir.toUri)
    loader.initTable()

    val pageviewsDay1 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-13 00:20:00")),
      Pageview("user-1", "news/uk", Timestamp.valueOf("2019-03-13 00:20:10")),
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-13 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-13 21:00:00"))
    )

    loader.insert(pageviewsDay1.toDS().coalesce(2))

    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1

    val pageviewsDay2 = List(
      Pageview("user-2", "news/politics", Timestamp.valueOf("2019-03-14 13:00:00")),
      Pageview("user-3", "sport/handball", Timestamp.valueOf("2019-03-14 21:00:00")),
      Pageview("user-4", "business", Timestamp.valueOf("2019-03-14 14:00:00"))
    )

    loader.insert(pageviewsDay2.toDS().coalesce(2))
    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1 ++ pageviewsDay2

    val pageviewsDay3 = List(
      Pageview("user-1", "news/politics", Timestamp.valueOf("2019-03-15 00:20:00")),
      Pageview("user-2", "news/politics/budget", Timestamp.valueOf("2019-03-15 00:10:00")),
      Pageview("user-3", "sport/football", Timestamp.valueOf("2019-03-15 21:00:00"))
    )

    loader.insert(pageviewsDay3.toDS().coalesce(2))

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

    versionDirs(tableUri, "date=2019-03-13") shouldBe List("v1")
    versionDirs(tableUri, "date=2019-03-14") shouldBe List("v1")
    versionDirs(tableUri, "date=2019-03-15") shouldBe List("v1")

    // Rewrite pageviews to remove one of the identity IDs, affecting only day 2
    val updatedPageviewsDay2 = pageviewsDay2.filter(_.id != "user-4")
    loader.insert(updatedPageviewsDay2.toDS().coalesce(2))

    // Query to check we see the updated data
    loader.pageviews().collect() should contain theSameElementsAs pageviewsDay1 ++ updatedPageviewsDay2 ++ pageviewsDay3

    // Check underlying storage that we have both versions for the updated partition
    versionDirs(tableUri, "date=2019-03-13") shouldBe List("v1")
    versionDirs(tableUri, "date=2019-03-14") shouldBe List("v1", "v2")
    versionDirs(tableUri, "date=2019-03-15") shouldBe List("v1")

    // TODO: Query Metastore directly to check what partitions the table has?
    //   (When implementing rollback and creating views on historical versions we could just test that functionality
    //    instead of querying storage directly)

    spark.read
      .parquet(tableUri.toString + "2019-03-14/v1")
      .as[Pageview]
      .collect() should contain theSameElementsAs pageviewsDay2
  }

  def versionDirs(tableLocation: URI, partition: String): List[String] = {
    val dir = Paths.get(s"$tableLocation/$partition")
    val dirList = Option(dir.toFile.list()).map(_.toList).getOrElse(Nil)
    dirList.filter(_.matches("v\\d+"))
  }

}
