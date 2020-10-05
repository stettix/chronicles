package dev.chronicles.filebacked

import java.time.Instant

import org.scalatest.{FlatSpec, Matchers}
import cats.effect._

class TimestampsSpec extends FlatSpec with Matchers {

  val testStream = Timestamps.uniqueTimestamps[IO].take(1000)

  "A stream of timestamps" should "have all distinct values" in {
    val values = testStream.compile.toVector.unsafeRunSync()
    values.distinct should contain theSameElementsInOrderAs values
  }

  it should "ensure subsequent timestamps are equal or in increasing order" in {
    val timestampPairs = testStream.map(_._1).zipWithPrevious.compile.toVector.unsafeRunSync()

    timestampPairs.foreach {
      case (Some(prev), next) => assert(!prev.isAfter(next))
      case (None, _)          =>
    }

  }

}
