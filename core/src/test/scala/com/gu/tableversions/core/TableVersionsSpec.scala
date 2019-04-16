package com.gu.tableversions.core

import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions.PartitionOperation.{AddPartitionVersion, RemovePartition}
import com.gu.tableversions.core.TableVersions._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Spec containing tests that apply across all TableVersions implementations.
  *
  * These are black box tests purely in terms of the TableVersions interface.
  */
trait TableVersionsSpec {
  this: FlatSpec with Matchers =>

  def tableVersionsBehaviour(emptyTableVersions: IO[TableVersions[IO]]): Unit = {

    val table = TableName("schema", "table")
    val userId = UserId("Test user")
    val date = PartitionColumn("date")

    it should "have an idempotent 'init' operation" in {

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)
        tableVersion1 <- tableVersions.currentVersion(table)

        _ <- tableVersions.init(table)
        tableVersion2 <- tableVersions.currentVersion(table)

        _ <- tableVersions.init(table)
        tableVersion3 <- tableVersions.currentVersion(table)

      } yield (tableVersion1, tableVersion2, tableVersion3)

      val (tableVersion1, tableVersion2, tableVersion3) = scenario.unsafeRunSync()

      tableVersion1 shouldBe TableVersion.empty
      tableVersion2 shouldBe tableVersion1
      tableVersion3 shouldBe tableVersion1
    }

    it should "allow partition versions of a partitioned table to be updated and queried" in {

      val initialPartitionVersions = Map(
        Partition(date, "2019-03-01") -> Version(1),
        Partition(date, "2019-03-02") -> Version(1)
      )

      val partitionUpdate1 = Map(
        Partition(date, "2019-03-02") -> Version(2),
        Partition(date, "2019-03-03") -> Version(1)
      )

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)

        initialTableVersion <- tableVersions.currentVersion(table)

        // Add some partitions
        commitResult1 <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion.tupled).toList)
        )

        version1 <- tableVersions.currentVersion(table)

        // Do an update with one updated partition and one new one
        nextVersions1 <- tableVersions.nextVersions(table, partitionUpdate1.keys.toList)
        commitResult2 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("First update"),
                                                          timestamp(2),
                                                          partitionUpdate1.map(AddPartitionVersion.tupled).toList))
        version2 <- tableVersions.currentVersion(table)

      } yield (initialTableVersion, commitResult1, version1, nextVersions1, commitResult2, version2)

      val (initialTableVersion, commitResult1, version1, nextVersions1, commitResult2, version2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe TableVersion.empty
      commitResult1 shouldBe SuccessfulCommit
      version1 shouldBe TableVersion(initialPartitionVersions)

      nextVersions1 shouldBe partitionUpdate1
      commitResult2 shouldBe SuccessfulCommit
      version2.partitionVersions shouldEqual Map(
        Partition(date, "2019-03-01") -> Version(1),
        Partition(date, "2019-03-02") -> Version(2),
        Partition(date, "2019-03-03") -> Version(1)
      )
    }

    it should "allow partitions to be removed from a partitioned table" in {
      val initialPartitionVersions = Map(
        Partition(date, "2019-03-01") -> Version(1),
        Partition(date, "2019-03-02") -> Version(1)
      )

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)

        // Add some partitions
        _ <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion.tupled).toList)
        )

        // Remove one of the partitions
        _ <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(2),
                      List(RemovePartition(Partition(date, "2019-03-01"))))
        )

        versionAfterRemove <- tableVersions.currentVersion(table)
        nextVersionsAfterRemove <- tableVersions.nextVersions(table, List(Partition(date, "2019-03-01")))

        // Re-add the removed partition
        _ <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("First update"),
                      timestamp(3),
                      List(AddPartitionVersion(Partition(date, "2019-03-01"), Version(2))))
        )

        versionAfterReAdd <- tableVersions.currentVersion(table)

      } yield (versionAfterRemove, nextVersionsAfterRemove, versionAfterReAdd)

      val (versionAfterRemove, nextVersionsAfterRemove, versionAfterReAdd) =
        scenario.unsafeRunSync()

      versionAfterRemove.partitionVersions shouldBe Map(Partition(date, "2019-03-02") -> Version(1))

      // Note: after re-adding a removed partition, the new version needs to be distinct from the old, removed one.
      nextVersionsAfterRemove shouldBe Map(
        Partition(date, "2019-03-01") -> Version(2)
      )

      versionAfterReAdd.partitionVersions shouldEqual Map(
        Partition(date, "2019-03-01") -> Version(2),
        Partition(date, "2019-03-02") -> Version(1)
      )
    }

    it should "allow versions of a snapshot table to be updated and queried" in {

      val version1 = TableVersion.snapshotVersion(Version(1))
      val version2 = TableVersion.snapshotVersion(Version(2))

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table)
        initialTableVersion <- tableVersions.currentVersion(table)

        nextVersion1 <- tableVersions.nextVersions(table, List(Partition.snapshotPartition))
        commitResult1 <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("First commit"),
                      timestamp(1),
                      version1.partitionVersions.map(AddPartitionVersion.tupled).toList))
        currentVersion1 <- tableVersions.currentVersion(table)

        nextVersion2 <- tableVersions.nextVersions(table, List(Partition.snapshotPartition))
        commitResult2 <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Second commit"),
                      timestamp(2),
                      version1.partitionVersions.map(AddPartitionVersion.tupled).toList))
        currentVersion2 <- tableVersions.currentVersion(table)

      } yield
        (initialTableVersion,
         nextVersion1,
         commitResult1,
         currentVersion1,
         nextVersion2,
         commitResult2,
         currentVersion2)

      val (initialTableVersion,
           nextVersion1,
           commitResult1,
           currentVersion1,
           nextVersion2,
           commitResult2,
           currentVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe TableVersion.empty
      nextVersion1 shouldBe version1.partitionVersions

      commitResult1 shouldBe SuccessfulCommit
      currentVersion1 shouldBe version1

      nextVersion2 shouldBe version2.partitionVersions
      commitResult2 shouldBe SuccessfulCommit
      currentVersion2 shouldBe currentVersion2
    }

    it should "return an error if trying to get current version of an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions
        version <- tableVersions.currentVersion(table)
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "schema.*table.*not found"
    }

    it should "return an error if trying to get next versions from an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions
        version <- tableVersions.nextVersions(TableName("schema", "table"), List(Partition.snapshotPartition))
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "schema.*table.*not found"
    }

    it should "return an error if trying to commit changes for an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions

        version <- tableVersions.commit(
          TableName("schema", "table"),
          TableUpdate(userId,
                      UpdateMessage("Commit initial partitions"),
                      timestamp(1),
                      List(AddPartitionVersion(Partition.snapshotPartition, Version(1))))
        )
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

  }

  private def timestamp(tick: Long): Instant = Instant.ofEpochSecond(1553705295L + (tick * 60))

}
