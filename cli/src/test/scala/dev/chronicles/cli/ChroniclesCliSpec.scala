package dev.chronicles.cli

import cats.effect.{Clock, IO}
import cats.implicits._
import dev.chronicles.cli.StubConsole.{StdErr, StdOut}
import dev.chronicles.core.VersionTracker.UserId
import dev.chronicles.core.{InMemoryVersionTracker, TableName, VersionedMetastore}
import org.scalatest.{FlatSpec, Matchers}

class ChroniclesCliSpec extends FlatSpec with Matchers {

  "Parsing command line arguments" should "complain if no command is given" in {
    assert(ChroniclesCli.argParser.parse(Nil).isLeft)

    ChroniclesCli.argParser.parse(Nil).left.get.toString should include("Usage")
  }

  it should "build a ListTables action for a 'tables' command" in {
    val result = ChroniclesCli.argParser.parse(List("tables"))
    result shouldBe Right(Action.ListTables)
  }

  it should "return an error if unknown options are given to the 'tables' command" in {
    assert(ChroniclesCli.argParser.parse(List("tables", "foo")).isLeft)
  }

  it should "build an action for a 'log <table name>' command" in {
    val result = ChroniclesCli.argParser.parse(List("log", "schema.table_name"))
    result shouldBe Right(Action.ShowTableHistory(TableName("schema", "table_name")))
  }

  "Running commands to add and list tables" should "show all tables that exist" in {

    val scenario = for {
      console <- StubConsole[IO]
      client <- createClient(console, createClock)

      _ <- run(List("tables"), client, console)
      _ <- run(List("init", "db.test_table", "--message", "Initial commit"), client, console)
      _ <- run(List("tables"), client, console)

      consoleOutput <- console.output

    } yield consoleOutput

    val consoleOutput = scenario.unsafeRunSync()

    consoleOutput should contain theSameElementsInOrderAs List(
      StdOut(""),
      StdOut("Initialised table db.test_table"),
      StdOut("db.test_table")
    )
  }

  "Trying to add a table with an invalid table name" should "display a helpful error message" in {
    val scenario = for {
      console <- StubConsole[IO]
      client <- createClient(console, createClock)

      _ <- run(List("init", "invalid name", "--message", "Initial commit"), client, console)

      consoleOutput <- console.output

    } yield consoleOutput

    val consoleOutput = scenario.unsafeRunSync()
    consoleOutput.size shouldBe 1

    val errorOutput = consoleOutput.collect { case StdErr(line) => line }
    errorOutput.size shouldBe 1

    val error = errorOutput.head

    error should include("invalid name")
  }

  // Helper method to run a command and swallow any errors - used when
  // we're just interested in what's output on the console.
  private def run(
      args: List[String],
      client: CliClient[IO],
      console: Console[IO],
      userId: UserId = UserId("user-1")): IO[Unit] =
    ChroniclesCli
      .run(args, client, console, userId)
      .void
      .handleErrorWith(_ => IO.unit)

  private def createClient(console: Console[IO], clock: Clock[IO]): IO[CliClient[IO]] =
    for {
      versionTracker <- InMemoryVersionTracker[IO]
      metastore = new StubMetastore[IO]
    } yield new CliClient[IO](new VersionedMetastore[IO](versionTracker, metastore), console, clock)

  private val createClock: Clock[IO] = Clock.create[IO]

}
