package dev.chronicles.files.versiontracker

import org.scalatest.{FlatSpec, Matchers}
import cats.effect._

class TimestampsSpec extends FlatSpec with Matchers {

  val testStream = Timestamps.uniqueTimestamps[IO].take(1000)

  "A stream of timestamps" should "have all distinct values" in {
    val values = testStream.compile.toVector.unsafeRunSync()
    values.distinct should contain theSameElementsInOrderAs values
  }

  it should "ensure subsequent timestamps are in increasing order" in {
    val timestampPairs = testStream.zipWithPrevious.compile.toVector.unsafeRunSync()

    timestampPairs.foreach {
      case (Some(prev), next) => assert(next.isAfter(prev))
      case (None, _)          =>
    }

  }

}
