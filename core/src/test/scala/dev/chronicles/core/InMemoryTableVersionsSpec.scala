package dev.chronicles.core

import cats.effect.IO
import org.scalatest.{FlatSpec, Matchers}

class InMemoryTableVersionsSpec extends FlatSpec with Matchers with TableVersionsSpec {

  "The reference implementation for the TableVersions service" should behave like tableVersionsBehaviour {
    InMemoryTableVersions[IO]
  }

}
