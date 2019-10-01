package dev.chronicles.db

import cats.effect._
import dev.chronicles.core.TableName
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

}
