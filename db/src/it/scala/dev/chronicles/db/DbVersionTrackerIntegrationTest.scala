package dev.chronicles.db

import cats.effect._
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import org.scalatest.{FlatSpec, Matchers}

class DbVersionTrackerIntegrationTest extends FlatSpec with Matchers with doobie.scalatest.IOChecker {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  override val transactor: Aux[IO, Unit] =
    Transactor.fromDriverManager[IO]("org.h2.Driver", "jdbc:h2:mem:", "", "")

  val versionTracker = new DbVersionTracker(transactor)

  "Initialising the version tracker" should "produce the right tables" in {
    // TODO: this test will probably be redundant when I have tests that actually perform operations
    val scenario = for {
      _ <- versionTracker.init()
      _ <- versionTracker.init()
    } yield ()

    scenario.unsafeRunSync()
  }
  // TODO: Check queries using IOChecker (see https://tpolecat.github.io/doobie/docs/13-Unit-Testing.html)

}