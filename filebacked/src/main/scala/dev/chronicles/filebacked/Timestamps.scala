package dev.chronicles.filebacked

import java.time.Instant

import cats.effect._
import cats.implicits._
import fs2.Stream

object Timestamps {

  /**
    * Produces a stream that contains pairs of timestamps and discriminators.
    *
    * The timestamps are guaranteed to be greater than or equal to previous values in the stream.
    * If a timestamp has not appeared before in the stream, its corresponding discriminator is None.
    * If a timestamp has appeared before, the corresponding discriminator value is guaranteed to be unique.
    */
  def uniqueTimestamps[F[_]](implicit F: Sync[F]): Stream[F, (Instant, Option[Int])] = {

    val timestamps: Stream[F, Long] =
      Stream.eval(F.delay(System.currentTimeMillis())).repeat

    val timestampsWithDiscriminator: Stream[F, (Long, Int)] = timestamps.groupAdjacentBy(identity).flatMap {
      case (_, ts) if ts.size > 1 => Stream.emits(ts.toList.zipWithIndex)
      case (t, _)                  => Stream.emit(t -> 0)
    }

    timestampsWithDiscriminator.map {
      case (ts, idx) => Instant.ofEpochMilli(ts) -> (if (idx == 0) None else Some(idx))
    }
  }

}
