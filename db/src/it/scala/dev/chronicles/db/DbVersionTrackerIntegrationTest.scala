package dev.chronicles.db

import java.time.Instant

import cats.effect._
import dev.chronicles.core.TableName
import dev.chronicles.core.VersionTracker._
import doobie.util.transactor.Transactor
import org.scalatest.{FlatSpec, Matchers}

class DbVersionTrackerIntegrationTest extends FlatSpec with Matchers with doobie.scalatest.IOChecker {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  override val transactor: Transactor[IO] =
    Transactor.fromDriverManager[IO]("org.h2.Driver", "jdbc:h2:mem:itestdb;DB_CLOSE_DELAY=-1", "", "")

  val versionTracker = DbVersionTracker(transactor)

  val table = TableName("db", "test")

  DbVersionTracker.initTables(transactor).unsafeRunSync()

  "getAllTables" should "be valid" in { check(DbVersionTracker.getAllTables) }
  "getTableMetadata" should "be valid" in { check(DbVersionTracker.getTableMetadata(table)) }
  "getUpdates" should "be valid" in { check(DbVersionTracker.getUpdates(table)) }
  "addTable" should "be valid" in {
    check(
      DbVersionTracker
        .addTable(table, CommitId("id"), Instant.now(), UserId("id"), UpdateMessage("message"), isSnapshot = false))
  }
  "getCurrentVersion" should "be valid" in { check(DbVersionTracker.getCurrentVersion(table)) }
  "initialiseCurrentVersion" should "be valid" in {
    check(DbVersionTracker.initialiseCurrentVersion(table, CommitId("id")))
  }
  "updateCurrentVersion" should "be valid" in {
    check(DbVersionTracker.updateCurrentVersion(table, CommitId("id")))
  }
  "getCommit" should "be valid" in { check(DbVersionTracker.getCommit(CommitId("id"))) }

}
