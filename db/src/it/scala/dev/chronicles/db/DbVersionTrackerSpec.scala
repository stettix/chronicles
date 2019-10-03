package dev.chronicles.db

import cats.effect.{ContextShift, IO}
import cats.implicits._
import dev.chronicles.core.VersionTrackerSpec
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import org.scalatest.{FlatSpec, Matchers}

class DbVersionTrackerSpec extends FlatSpec with Matchers with VersionTrackerSpec {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  val transactor =
    Transactor.fromDriverManager[IO]("org.h2.Driver", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "", "")

  private val clearDatabaseState: IO[Unit] = {
    // We have to reuse an in-memory across transactions in these test so we can't use a short-lived h2 database.
    // This means we have to drop any lingering state if it exists before a new test starts.
    val tables = List(
      "chronicle_tables_v1",
      "chronicle_table_updates_v1",
      "chronicle_table_operations_v1",
      "chronicles_version_refs_v1"
    )

    val ops = tables.traverse { table =>
      val sql = fr"""drop table if exists""" ++ Fragment.const(table)
      sql.update.run
    }

    ops.transact(transactor).void
  }

  "The database-backed version tracker implementation" should behave like versionTrackerBehaviour(createTracker)

  private def createTracker: IO[DbVersionTracker[IO]] = {
    val tracker = new DbVersionTracker[IO](transactor)
    for {
      _ <- clearDatabaseState
      _ <- tracker.init()
    } yield tracker

  }

}
