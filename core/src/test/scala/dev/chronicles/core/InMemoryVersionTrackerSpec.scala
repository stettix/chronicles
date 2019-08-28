package dev.chronicles.core

import cats.effect.IO
import org.scalatest.{FlatSpec, Matchers}

class InMemoryVersionTrackerSpec extends FlatSpec with Matchers with VersionTrackerSpec {

  "The reference implementation for the service" should behave like versionTrackerBehaviour {
    InMemoryVersionTracker[IO]
  }

}
