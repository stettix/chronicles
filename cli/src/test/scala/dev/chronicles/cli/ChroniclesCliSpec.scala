package dev.chronicles.cli

import org.scalatest.{FlatSpec, Matchers}

class ChroniclesCliSpec extends FlatSpec with Matchers {

  "Parsing command line arguments" should "complain if no command is given" in {
    assert(ChroniclesCli.argParser.parse(Nil).isLeft)

    println(ChroniclesCli.argParser.parse(Nil).left.get.toString)
  }

  it should "build a ListTables action for a 'tables' command" in {
    val result = ChroniclesCli.argParser.parse(List("tables"))
    result shouldBe Right(Action.ListTables)
  }

  it should "return an error if unknown options are given to the 'tables' command" in {
    assert(ChroniclesCli.argParser.parse(List("tables", "foo")).isLeft)
  }

  it should "build a Log action for a 'log <table name>' command" in {
    val result = ChroniclesCli.argParser.parse(List("log", "table_name"))
    result shouldBe Right(Action.ShowTableHistory("table_name"))
  }

}
