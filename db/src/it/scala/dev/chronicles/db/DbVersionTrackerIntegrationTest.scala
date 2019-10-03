package dev.chronicles.db

import java.time.Instant

import cats.effect._
import dev.chronicles.core.TableName
import dev.chronicles.core.VersionTracker._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.scalatest.{FlatSpec, Matchers}

class DbVersionTrackerIntegrationTest extends FlatSpec with Matchers with doobie.scalatest.IOChecker {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  override val transactor: Aux[IO, Unit] =
    Transactor.fromDriverManager[IO]("org.h2.Driver", "jdbc:h2:mem:itestdb;DB_CLOSE_DELAY=-1", "", "")

  val versionTracker = new DbVersionTracker(transactor)

  val table = TableName("db", "test")

  DbVersionTracker.initTables(transactor).unsafeRunSync()

  "allTablesQuery" should "be valid" in { check(DbVersionTracker.allTablesQuery) }
  "tableCountQuery" should "be valid" in { check(DbVersionTracker.tableCountQuery(table)) }
  "tableMetadataQuery" should "be valid" in { check(DbVersionTracker.tableMetadataQuery(table)) }
  "updatesQuery" should "be valid" in { check(DbVersionTracker.updatesQuery(table)) }
  "addTableQuery" should "be valid" in {
    check(DbVersionTracker
      .addTableUpdate(table, CommitId("id"), Instant.now(), UserId("id"), UpdateMessage("message"), isSnapshot = false))
  }
  "currentVersionQuery" should "be valid" in { check(DbVersionTracker.currentVersionQuery(table)) }
  "setCurrentVersionUpdate" should "be valid" in {
    check(DbVersionTracker.initialiseCurrentVersionUpdate(table, CommitId("id")))
  }
  "updateCurrentVersionUpdate" should "be valid" in {
    check(DbVersionTracker.updateCurrentVersionUpdate(table, CommitId("id")))
  }
  "getCommit" should "be valid" in { check(DbVersionTracker.getCommit(CommitId("id"))) }

}
