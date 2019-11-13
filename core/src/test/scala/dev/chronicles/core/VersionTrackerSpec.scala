package dev.chronicles.core

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import dev.chronicles.core.Partition.PartitionColumn
import dev.chronicles.core.VersionTracker.TableOperation.{AddPartitionVersion, AddTableVersion, RemovePartition}
import dev.chronicles.core.VersionTracker.{CommitId, TableUpdate, UpdateMessage, UserId}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/**
  * Spec containing tests that apply across all implementations.
  *
  * These are black box tests purely in terms of the common interface.
  */
trait VersionTrackerSpec {
  this: FlatSpec with Matchers =>

  val version1 = Version.generateVersion[IO].unsafeRunSync()
  val version2 = Version.generateVersion[IO].unsafeRunSync()
  val version3 = Version.generateVersion[IO].unsafeRunSync()

  def versionTrackerBehaviour(initialVersionTracker: IO[VersionTracker[IO]]): Unit = {

    val table = TableName("schema", "table")

    val userId = UserId("Test user")
    val date = PartitionColumn("date")

    it should "initialise tables" in {

      val table2 = TableName("schema", "table2")

      val scenario = for {
        versionTracker <- initialVersionTracker

        tablesBeforeInit <- versionTracker.tables().compile.toList

        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init table 1"), Instant.now())
        tables1 <- versionTracker.tables().compile.toList

        _ <- versionTracker.initTable(table2, isSnapshot = false, userId, UpdateMessage("init table 2"), Instant.now())
        tables2 <- versionTracker.tables().compile.toList

      } yield (tablesBeforeInit, tables1, tables2)

      val (tablesBeforeInit, tables1, tables2) = scenario.unsafeRunSync()

      tablesBeforeInit shouldBe Nil
      tables1 should contain theSameElementsAs List(table)
      tables2 should contain theSameElementsAs List(table, table2)
    }

    it should "have an idempotent 'init' operation" in {

      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init1"), Instant.now())
        tableVersion1 <- versionTracker.currentVersion(table)

        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init2"), Instant.now())
        tableVersion2 <- versionTracker.currentVersion(table)

        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init3"), Instant.now())
        tableVersion3 <- versionTracker.currentVersion(table)

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
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init"), Instant.now())

        initialTableVersion <- versionTracker.currentVersion(table)

        // Add some partitions
        _ <- versionTracker.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion.tupled).toList)
        )

        updates1 <- versionTracker.updates(table).compile.toList

        tableVersion1 <- versionTracker.currentVersion(table)

        // Do an update with one updated partition and one new one
        _ <- versionTracker.commit(table,
                                   TableUpdate(userId,
                                               UpdateMessage("First update"),
                                               timestamp(2),
                                               partitionUpdate1.map(AddPartitionVersion.tupled).toList))
        tableVersion2 <- versionTracker.currentVersion(table)

      } yield (initialTableVersion, updates1, tableVersion1, tableVersion2)

      val (initialTableVersion, updates1, tableVersion1, tableVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe PartitionedTableVersion(Map.empty)

      updates1.map(_.message.content) should contain theSameElementsAs List("Add initial partitions", "init")
      tableVersion1 shouldBe PartitionedTableVersion(initialPartitionVersions)

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
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init"), Instant.now())

        // Add some partitions
        _ <- versionTracker.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(1),
                      initialPartitionVersions.map(AddPartitionVersion.tupled).toList)
        )

        // Remove one of the partitions
        _ <- versionTracker.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("Add initial partitions"),
                      timestamp(2),
                      List(RemovePartition(Partition(date, "2019-03-01"))))
        )

        versionAfterRemove <- versionTracker.currentVersion(table)

        // Re-add the removed partition
        _ <- versionTracker.commit(
          table,
          TableUpdate(userId,
                      UpdateMessage("First update"),
                      timestamp(3),
                      List(AddPartitionVersion(Partition(date, "2019-03-01"), version2)))
        )

        versionAfterReAdd <- versionTracker.currentVersion(table)

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
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())
        initialTableVersion <- versionTracker.currentVersion(table)

        commitResult1 <- versionTracker.commit(table,
                                               TableUpdate(userId,
                                                           UpdateMessage("First commit"),
                                                           timestamp(1),
                                                           List(AddTableVersion(tableVersion1.version))))
        currentVersion1 <- versionTracker.currentVersion(table)

        commitResult2 <- versionTracker.commit(table,
                                               TableUpdate(userId,
                                                           UpdateMessage("Second commit"),
                                                           timestamp(2),
                                                           List(AddTableVersion(tableVersion2.version))))
        currentVersion2 <- versionTracker.currentVersion(table)

      } yield (initialTableVersion, commitResult1, currentVersion1, commitResult2, currentVersion2)

      val (initialTableVersion, commitResult1, currentVersion1, commitResult2, currentVersion2) =
        scenario.unsafeRunSync()

      initialTableVersion shouldBe SnapshotTableVersion(Version.Unversioned)
      currentVersion1 shouldBe tableVersion1
      currentVersion2 shouldBe currentVersion2
    }

    it should "allow table versions of a partitioned table to be updated from version history" in {

      val partition1 = Partition(date, "2019-03-01")
      val partition2 = Partition(date, "2019-03-02")

      val tableUpdate1 = TableUpdate(userId,
                                     UpdateMessage("Day 1 first commit"),
                                     timestamp(1),
                                     List(AddPartitionVersion(partition1, version1)))
      val tableUpdate2 = TableUpdate(userId,
                                     UpdateMessage("Day 2 first commit"),
                                     timestamp(2),
                                     List(AddPartitionVersion(partition2, version1)))

      val tableUpdate3 = TableUpdate(userId,
                                     UpdateMessage("Day 1 reprocessed"),
                                     timestamp(3),
                                     List(AddPartitionVersion(partition1, version2)))

      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = false, userId, UpdateMessage("init"), timestamp(0))

        historyAfterInit <- versionTracker.updates(table).compile.toList

        _ <- versionTracker.commit(table, tableUpdate1)
        _ <- versionTracker.commit(table, tableUpdate2)
        _ <- versionTracker.commit(table, tableUpdate3)

        versionAfterWrites <- versionTracker.currentVersion(table)

        // Get the update history after updates
        fullHistory <- versionTracker.updates(table).compile.toList

        // Setting the current version to the latest should have no effect
        _ <- versionTracker.setCurrentVersion(table, fullHistory.head.id)
        versionAfterWrites2 <- versionTracker.currentVersion(table)

        // Set the initial version, i.e. the table as it was before any committed version
        _ <- versionTracker.setCurrentVersion(table, fullHistory.last.id)
        versionSetToInitial <- versionTracker.currentVersion(table)

        // Set the version after the first commit
        _ <- versionTracker.setCurrentVersion(table, fullHistory.takeRight(2).head.id)
        versionSetToFirst <- versionTracker.currentVersion(table)

        // Set the second from last version
        _ <- versionTracker.setCurrentVersion(table, fullHistory.drop(1).head.id)
        versionSetToSecond <- versionTracker.currentVersion(table)

        // Set the latest version again
        _ <- versionTracker.setCurrentVersion(table, fullHistory.head.id)
        versionSetToLatest <- versionTracker.currentVersion(table)

      } yield
        (historyAfterInit,
         fullHistory,
         versionAfterWrites,
         versionAfterWrites2,
         versionSetToInitial,
         versionSetToFirst,
         versionSetToSecond,
         versionSetToLatest)

      val (historyAfterInit,
           fullHistory,
           versionAfterWrites,
           versionAfterWrites2,
           versionSetToInitial,
           versionSetToFirst,
           versionSetToSecond,
           versionSetToLatest) = scenario.unsafeRunSync()

      historyAfterInit should have size 1
      val initUpdate = historyAfterInit.head
      initUpdate.userId shouldBe userId
      initUpdate.message shouldBe UpdateMessage("init")
      initUpdate.timestamp shouldBe timestamp(0)

      // Log should return updates most recent first
      fullHistory should contain theSameElementsInOrderAs List(tableUpdate3.metadata,
                                                               tableUpdate2.metadata,
                                                               tableUpdate1.metadata,
                                                               initUpdate)
      // Commit IDs should be unique
      fullHistory.map(_.id).distinct should contain theSameElementsAs fullHistory.map(_.id)

      versionAfterWrites shouldBe PartitionedTableVersion(Map(partition1 -> version2, partition2 -> version1))
      versionAfterWrites2 shouldEqual versionAfterWrites

      versionSetToInitial shouldBe PartitionedTableVersion(Map.empty)
      versionSetToFirst shouldBe PartitionedTableVersion(Map(partition1 -> version1))
      versionSetToSecond shouldBe PartitionedTableVersion(Map(partition1 -> version1, partition2 -> version1))
      versionSetToLatest shouldEqual versionAfterWrites
    }

    it should "allow table versions of a snapshot table to be updated from version history" in {

      val tableUpdate1 =
        TableUpdate(userId, UpdateMessage("First commit"), timestamp(1), List(AddTableVersion(version1)))

      val tableUpdate2 =
        TableUpdate(userId, UpdateMessage("Second commit"), timestamp(2), List(AddTableVersion(version2)))

      val tableUpdate3 =
        TableUpdate(userId, UpdateMessage("Third commit"), timestamp(3), List(AddTableVersion(version3)))

      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())

        historyAfterInit <- versionTracker.updates(table).compile.toList

        _ <- versionTracker.commit(table, tableUpdate1)
        _ <- versionTracker.commit(table, tableUpdate2)
        _ <- versionTracker.commit(table, tableUpdate3)

        versionAfterWrites <- versionTracker.currentVersion(table)

        // Get the update history after updates
        fullHistory <- versionTracker.updates(table).compile.toList

        // Setting the current version to the latest should have no effect
        _ <- versionTracker.setCurrentVersion(table, fullHistory.head.id)
        versionAfterWrites2 <- versionTracker.currentVersion(table)

        // Set the initial version, i.e. the table as it was before any committed version
        _ <- versionTracker.setCurrentVersion(table, fullHistory.last.id)
        versionSetToInitial <- versionTracker.currentVersion(table)

        // Set the version after the first commit
        _ <- versionTracker.setCurrentVersion(table, fullHistory.takeRight(2).head.id)
        versionSetToFirst <- versionTracker.currentVersion(table)

        // Set the second from last version
        _ <- versionTracker.setCurrentVersion(table, fullHistory.drop(1).head.id)
        versionSetToSecond <- versionTracker.currentVersion(table)

        // Set the latest version again
        _ <- versionTracker.setCurrentVersion(table, fullHistory.head.id)
        versionSetToLatest <- versionTracker.currentVersion(table)

      } yield
        (historyAfterInit,
         fullHistory,
         versionAfterWrites,
         versionAfterWrites2,
         versionSetToInitial,
         versionSetToFirst,
         versionSetToSecond,
         versionSetToLatest)

      val (historyAfterInit,
           fullHistory,
           versionAfterWrites,
           versionAfterWrites2,
           versionSetToInitial,
           versionSetToFirst,
           versionSetToSecond,
           versionSetToLatest) = scenario.unsafeRunSync()

      historyAfterInit should have size 1
      val initUpdate = historyAfterInit.head

      fullHistory should contain theSameElementsInOrderAs List(tableUpdate3.metadata,
                                                               tableUpdate2.metadata,
                                                               tableUpdate1.metadata,
                                                               initUpdate)

      // Commit IDs should be unique
      fullHistory.map(_.id).distinct should contain theSameElementsAs fullHistory.map(_.id)

      versionAfterWrites shouldBe SnapshotTableVersion(version3)
      versionAfterWrites2 shouldEqual versionAfterWrites

      versionSetToInitial shouldBe SnapshotTableVersion(Version.Unversioned)
      versionSetToFirst shouldBe SnapshotTableVersion(version1)
      versionSetToSecond shouldBe SnapshotTableVersion(version2)
      versionSetToLatest shouldEqual versionAfterWrites
    }

    it should "return updates in the same order as they were committed, but with the most recent first" in {
      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())

        // Generate some updates
        indexedVersions <- (1 to 100).map(n => Version.generateVersion[IO].map(v => n -> v)).toList.sequence
        initialUpdates = indexedVersions.map {
          case (n, version) =>
            TableUpdate(userId, UpdateMessage(s"Commit number $n"), timestamp(n.toLong), List(AddTableVersion(version)))
        }

        tableUpdates = Random.shuffle(initialUpdates)

        // Commit all the updates
        _ <- tableUpdates.map(update => versionTracker.commit(table, update)).sequence

        updateHistory <- versionTracker.updates(table).compile.toList

      } yield (tableUpdates, updateHistory)

      val (committedUpdates, updateHistory) = scenario.unsafeRunSync()
      val updateHistoryWithoutInitOperation = updateHistory.dropRight(1)

      val expectedUpdates = committedUpdates.map(_.metadata.id).reverse
      updateHistoryWithoutInitOperation.map(_.id) should contain theSameElementsInOrderAs expectedUpdates
      updateHistoryWithoutInitOperation.map(_.id) should contain theSameElementsInOrderAs expectedUpdates
    }

    it should "return an error if trying to get current version of an unknown table" in {
      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.currentVersion(table)
      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

    it should "return an error if trying to commit changes for an unknown table" in {
      val scenario = for {
        versionTracker <- initialVersionTracker

        _ <- versionTracker.commit(
          TableName("schema", "table"),
          TableUpdate(userId, UpdateMessage("Commit initial partitions"), timestamp(1), List(AddTableVersion(version1)))
        )
      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

    it should "return an error if trying to get the version history for an unknown table" in {
      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.updates(table).compile.toList
      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

    it should "return an error if trying to set the version of an unknown table" in {
      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.setCurrentVersion(table, CommitId("unknown-commit-id"))

      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown table.*schema.*table"
    }

    it should "return an error if trying to set the version of a table to an unknown commit ID" in {
      val scenario = for {
        versionTracker <- initialVersionTracker
        _ <- versionTracker.initTable(table, isSnapshot = true, userId, UpdateMessage("init"), Instant.now())

        _ <- versionTracker.setCurrentVersion(table, CommitId("unknown-commit-id"))

      } yield ()

      val ex = the[Exception] thrownBy scenario.unsafeRunSync()
      ex.getMessage should include regex "Unknown commit.*unknown-commit-id"
    }

  }

  private def timestamp(tick: Long): Instant = Instant.ofEpochSecond(1553705295L + (tick * 60))

}
