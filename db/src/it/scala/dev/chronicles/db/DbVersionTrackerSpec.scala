package dev.chronicles.db

import cats.effect.{ContextShift, IO}
import dev.chronicles.core.VersionTrackerSpec
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.scalatest.{FlatSpec, Matchers}

class DbVersionTrackerSpec extends FlatSpec with Matchers with VersionTrackerSpec {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  val transactor =
    Transactor.fromDriverManager[IO]("org.h2.Driver", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "", "")

  "The database-backed version tracker implementation" should behave like versionTrackerBehaviour {
    val tracker = new DbVersionTracker[IO](transactor)
    tracker.init().map(_ => tracker)
  }

}
