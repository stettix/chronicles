package com.gu.tableversions.metastore

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core._
import com.gu.tableversions.metastore.Metastore.TableChanges
import com.gu.tableversions.metastore.Metastore.TableOperation._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Spec containing tests that apply across all Metastore implementations.
  *
  * These are black box tests purely in terms of the Metastore interface.
  */
trait MetastoreSpec {
  this: FlatSpec with Matchers =>

  def metastoreWithSnapshotSupport(
      emptyMetastore: IO[Metastore[IO]],
      initHiveTable: IO[Unit],
      table: TableName): Unit = {

    it should "allow table versions to be updated for snapshot tables" in {

      val scenario = for {
        metastore <- emptyMetastore
        _ <- initHiveTable

        initialVersion <- metastore.currentVersion(table)

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(Version(1)))))

        firstUpdatedVersion <- metastore.currentVersion(table)

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(Version(42)))))

        secondUpdatedVersion <- metastore.currentVersion(table)

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(Version(1)))))

        revertedVersion <- metastore.currentVersion(table)

      } yield (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion)

      val (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion) = scenario.unsafeRunSync()

      initialVersion shouldBe SnapshotTableVersion(Version(0))
      firstUpdatedVersion shouldBe
        SnapshotTableVersion(Version(1))
      secondUpdatedVersion shouldBe
        SnapshotTableVersion(Version(42))
      revertedVersion shouldEqual firstUpdatedVersion
    }

  }

  def metastoreWithPartitionsSupport(
      emptyMetastore: IO[Metastore[IO]],
      initHiveTable: IO[Unit],
      table: TableName): Unit = {

    val dateCol = PartitionColumn("date")

    it should "allow individual partitions to be updated in partitioned tables" in {
      val scenario = for {
        metastore <- emptyMetastore
        _ <- initHiveTable

        initialVersion <- metastore.currentVersion(table)

        _ <- metastore.update(
          table,
          TableChanges(
            List(
              AddPartition(Partition(dateCol, "2019-03-01"), Version(0)),
              AddPartition(Partition(dateCol, "2019-03-02"), Version(1)),
              AddPartition(Partition(dateCol, "2019-03-03"), Version(1))
            )
          )
        )

        versionAfterFirstUpdate <- metastore.currentVersion(table)

        _ <- metastore.update(
          table,
          TableChanges(
            List(
              UpdatePartitionVersion(Partition(dateCol, "2019-03-01"), Version(1)),
              UpdatePartitionVersion(Partition(dateCol, "2019-03-03"), Version(2))
            )
          )
        )

        versionAfterSecondUpdate <- metastore.currentVersion(table)

        _ <- metastore.update(
          table,
          TableChanges(
            List(
              RemovePartition(Partition(dateCol, "2019-03-02"))
            )
          )
        )

        versionAfterPartitionRemoved <- metastore.currentVersion(table)

      } yield (initialVersion, versionAfterFirstUpdate, versionAfterSecondUpdate, versionAfterPartitionRemoved)

      val (initialVersion, versionAfterFirstUpdate, versionAfterSecondUpdate, versionAfterPartitionRemoved) =
        scenario.unsafeRunSync()

      initialVersion shouldBe PartitionedTableVersion(Map.empty)

      versionAfterFirstUpdate shouldBe
        PartitionedTableVersion(
          Map(
            Partition(dateCol, "2019-03-01") -> Version(0),
            Partition(dateCol, "2019-03-02") -> Version(1),
            Partition(dateCol, "2019-03-03") -> Version(1)
          ))

      versionAfterSecondUpdate shouldBe
        PartitionedTableVersion(
          Map(
            Partition(dateCol, "2019-03-01") -> Version(1),
            Partition(dateCol, "2019-03-02") -> Version(1),
            Partition(dateCol, "2019-03-03") -> Version(2)
          ))

      versionAfterPartitionRemoved shouldBe
        PartitionedTableVersion(
          Map(
            Partition(dateCol, "2019-03-01") -> Version(1),
            Partition(dateCol, "2019-03-03") -> Version(2)
          ))

    }

    it should "return an error if trying to get the version of an unknown table" in {
      val scenario = for {
        metastore <- emptyMetastore
        _ <- initHiveTable

        version <- metastore.currentVersion(TableName("unknown", "table"))

      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage.toLowerCase should include regex "unknown.*not found"
    }

    it should "not allow updating the version of an unknown partition" in {
      val scenario = for {
        metastore <- emptyMetastore
        _ <- initHiveTable
        _ <- metastore.update(
          table,
          TableChanges(
            List(
              UpdatePartitionVersion(Partition(dateCol, "2019-03-01"), Version(1))
            )
          )
        )

      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage.toLowerCase should include regex "partition not found"
    }

  }

}
