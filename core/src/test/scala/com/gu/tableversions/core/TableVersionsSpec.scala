package com.gu.tableversions.core

import java.time.Instant

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.CommitResult.SuccessfulCommit
import com.gu.tableversions.core.TableVersions.TableOperation.{AddPartitionVersion, AddTableVersion, RemovePartition}
import com.gu.tableversions.core.TableVersions._
import org.scalatest.{FlatSpec, Matchers}

/**
  * Spec containing tests that apply across all TableVersions implementations.
  *
  * These are black box tests purely in terms of the TableVersions interface.
  */
trait TableVersionsSpec {
  this: FlatSpec with Matchers =>
  val version1 = Version.generateVersion.unsafeRunSync()
  val version2 = Version.generateVersion.unsafeRunSync()

  def tableVersionsBehaviour(emptyTableVersions: IO[TableVersions[IO]]): Unit = {

    val table = TableName("schema", "table")
    val userId = UserId("Test user")
    val date = PartitionColumn("date")
    it should "have an idempotent 'init' operation" in {

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table, isSnapshot = false, userId, UpdateMessage("init1"), Instant.now())
        tableVersion1 <- tableVersions.currentVersion(table)

        _ <- tableVersions.init(table, isSnapshot = false, userId, UpdateMessage("init2"), Instant.now())
        tableVersion2 <- tableVersions.currentVersion(table)

        _ <- tableVersions.init(table, isSnapshot = false, userId, UpdateMessage("init3"), Instant.now())
        tableVersion3 <- tableVersions.currentVersion(table)

      } yield (tableVersion1, tableVersion2, tableVersion3)

      val (tableVersion1, tableVersion2, tableVersion3) = scenario.unsafeRunSync()

      tableVersion1 shouldBe PartitionedTableVersion(Map.empty)
      tableVersion2 shouldBe tableVersion1
      tableVersion3 shouldBe tableVersion1
    }

    it should "allow partition versions of a partitioned table to be updated and queried" in {

      val initialPartitionVersions = Map(
        Partition(date, "2019-03-01") -> version1,
        Partition(date, "2019-03-02") -> version1
      )

      val partitionUpdate1 = Map(
        Partition(date, "2019-03-02") -> version2,
        Partition(date, "2019-03-03") -> version1
      )

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table, isSnapshot = false, userId, UpdateMessage("init"), Instant.now())

        initialTableVersion <- tableVersions.currentVersion(table)

        // Add some partitions
        commitResult1 <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion.tupled).toList)
        )

        tableVersion1 <- tableVersions.currentVersion(table)

        // Do an update with one updated partition and one new one
        commitResult2 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("First update"),
                                                          timestamp(2),
                                                          partitionUpdate1.map(AddPartitionVersion.tupled).toList))
        tableVersion2 <- tableVersions.currentVersion(table)

      } yield (initialTableVersion, commitResult1, tableVersion1, commitResult2, tableVersion2)

      val (initialTableVersion, commitResult1, tableVersion1, commitResult2, tableVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe PartitionedTableVersion(Map.empty)
      commitResult1 shouldBe SuccessfulCommit
      tableVersion1 shouldBe PartitionedTableVersion(initialPartitionVersions)

      commitResult2 shouldBe SuccessfulCommit
      tableVersion2 shouldEqual PartitionedTableVersion(
        Map(
          Partition(date, "2019-03-01") -> version1,
          Partition(date, "2019-03-02") -> version2,
          Partition(date, "2019-03-03") -> version1
        ))
    }

    it should "allow partitions to be removed from a partitioned table" in {
      val initialPartitionVersions = Map(
        Partition(date, "2019-03-01") -> version1,
        Partition(date, "2019-03-02") -> version1
      )

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table, isSnapshot = false, userId, UpdateMessage("init"), Instant.now())

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

        // Re-add the removed partition
        _ <- tableVersions.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("First update"),
                      timestamp(3),
                      List(AddPartitionVersion(Partition(date, "2019-03-01"), version2)))
        )

        versionAfterReAdd <- tableVersions.currentVersion(table)

      } yield (versionAfterRemove, versionAfterReAdd)

      val (versionAfterRemove, versionAfterReAdd) =
        scenario.unsafeRunSync()

      versionAfterRemove shouldBe PartitionedTableVersion(Map(Partition(date, "2019-03-02") -> version1))

      // Note: after re-adding a removed partition, the new version needs to be distinct from the old, removed one.

      versionAfterReAdd shouldEqual PartitionedTableVersion(
        Map(
          Partition(date, "2019-03-01") -> version2,
          Partition(date, "2019-03-02") -> version1
        ))
    }

    it should "allow versions of a snapshot table to be updated and queried" in {

      val tableVersion1 = SnapshotTableVersion(version1)
      val tableVersion2 = SnapshotTableVersion(version2)

      val scenario = for {
        tableVersions <- emptyTableVersions
        _ <- tableVersions.init(table, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())
        initialTableVersion <- tableVersions.currentVersion(table)
        commitResult1 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("First commit"),
                                                          timestamp(1),
                                                          List(AddTableVersion(tableVersion1.version))))
        currentVersion1 <- tableVersions.currentVersion(table)

        commitResult2 <- tableVersions.commit(table,
                                              TableUpdate(userId,
                                                          UpdateMessage("Second commit"),
                                                          timestamp(2),
                                                          List(AddTableVersion(tableVersion2.version))))
        currentVersion2 <- tableVersions.currentVersion(table)

      } yield (initialTableVersion, commitResult1, currentVersion1, commitResult2, currentVersion2)

      val (initialTableVersion, commitResult1, currentVersion1, commitResult2, currentVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe SnapshotTableVersion(Version.Unversioned)

      commitResult1 shouldBe SuccessfulCommit
      currentVersion1 shouldBe tableVersion1

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

    it should "return an error if trying to commit changes for an unknown table" in {
      val scenario = for {
        tableVersions <- emptyTableVersions

        version <- tableVersions.commit(
          TableName("schema", "table"),
          TableUpdate(userId, UpdateMessage("Commit initial partitions"), timestamp(1), List(AddTableVersion(version1)))
        )
      } yield version

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

  }

  private def timestamp(tick: Long): Instant = Instant.ofEpochSecond(1553705295L + (tick * 60))

}
