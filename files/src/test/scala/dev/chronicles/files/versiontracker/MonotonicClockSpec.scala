package dev.chronicles.files.versiontracker

import cats.effect._
import cats.implicits._
import org.scalatest.{FlatSpec, Matchers}

class MonotonicClockSpec extends FlatSpec with Matchers {

  "Getting timestamps from a timestamp provider" should "produce distinct values" in {
    val NumTests = 1000L

    val test = for {
      tp <- MonotonicClock[IO]
      timestamps <- (1L to NumTests).toList.traverse(_ => tp.nextTimestamp)
    } yield timestamps

    val timestamps = test.unsafeRunSync()

    timestamps.distinct should have size NumTests
    timestamps.sortBy(_.toEpochMilli) should contain theSameElementsInOrderAs timestamps
  }

}
