package dev.chronicles.cli

import dev.chronicles.core.TableName
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

}
