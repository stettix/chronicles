package dev.chronicles.filebacked

import org.scalatest.{FlatSpec, Matchers}
import cats.effect._

class TimestampsSpec extends FlatSpec with Matchers {

  val testStream1 = Timestamps.distinctTimestamps[IO].take(1000)

  "A stream of timestamps" should "have all distinct values" in {
    val values = testStream1.compile.toVector.unsafeRunSync()
    values.distinct should contain theSameElementsInOrderAs values
  }

  it should "ensure subsequent timestamps are equal or in increasing order" in {
    val timestampPairs = testStream2.zipWithPrevious.compile.toVector.unsafeRunSync()

    timestampPairs.foreach {
      case (Some(prev), next) => assert(!prev.isAfter(next))
      case (None, _)          =>
    }

  }

  val testStream2 = Timestamps.uniqueInstants[IO].take(1000)

  "A stream of timestamps" should "have all distinct values" in {
    val values = testStream2.compile.toVector.unsafeRunSync()
    values.distinct should contain theSameElementsInOrderAs values
  }

  it should "ensure subsequent timestamps are equal or in increasing order" in {
    val timestampPairs = testStream2.zipWithPrevious.compile.toVector.unsafeRunSync()

    timestampPairs.foreach {
      case (Some(prev), next) => assert(!prev.isAfter(next))
      case (None, _)          =>
    }

  }

}
