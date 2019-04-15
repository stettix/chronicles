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

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(VersionNumber(1)))))

        firstUpdatedVersion <- metastore.currentVersion(table)

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(VersionNumber(42)))))

        secondUpdatedVersion <- metastore.currentVersion(table)

        _ <- metastore.update(table, TableChanges(List(UpdateTableVersion(VersionNumber(1)))))

        revertedVersion <- metastore.currentVersion(table)

      } yield (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion)

      val (initialVersion, firstUpdatedVersion, secondUpdatedVersion, revertedVersion) = scenario.unsafeRunSync()

      initialVersion shouldBe TableVersion(List(PartitionVersion(Partition.snapshotPartition, VersionNumber(0))))
      firstUpdatedVersion shouldBe
        TableVersion(List(PartitionVersion(Partition.snapshotPartition, VersionNumber(1))))
      secondUpdatedVersion shouldBe
        TableVersion(List(PartitionVersion(Partition.snapshotPartition, VersionNumber(42))))
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
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(0))),
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1))),
              AddPartition(PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(1)))
            )
          )
        )

        versionAfterFirstUpdate <- metastore.currentVersion(table)

        _ <- metastore.update(
          table,
          TableChanges(
            List(
              UpdatePartitionVersion(PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1))),
              UpdatePartitionVersion(PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2)))
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

      initialVersion shouldBe TableVersion.empty

      versionAfterFirstUpdate shouldBe
        TableVersion(
          List(
            PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(0)),
            PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1)),
            PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(1))
          ))

      versionAfterSecondUpdate shouldBe
        TableVersion(
          List(
            PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1)),
            PartitionVersion(Partition(dateCol, "2019-03-02"), VersionNumber(1)),
            PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2))
          ))

      versionAfterPartitionRemoved shouldBe
        TableVersion(
          List(
            PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1)),
            PartitionVersion(Partition(dateCol, "2019-03-03"), VersionNumber(2))
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
              UpdatePartitionVersion(PartitionVersion(Partition(dateCol, "2019-03-01"), VersionNumber(1)))
            )
          )
        )

      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage.toLowerCase should include regex "partition not found"
    }

  }

}
