package dev.chronicles.filebacked

import java.time.Instant

import cats.Eq
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
      case (t, _)                 => Stream.emit(t -> 0)
    }

    timestampsWithDiscriminator.map {
      case (ts, idx) => Instant.ofEpochMilli(ts) -> (if (idx == 0) None else Some(idx))
    }
  }

  def distinctTimestamps[F[_]](implicit F: Sync[F]): Stream[F, Instant] = {

    def next(prev: Long): F[Option[(Long, Long)]] =
      for {
        tsMillis <- F.delay(System.currentTimeMillis())
        next = if (prev < tsMillis) tsMillis else prev + 1
      } yield Some(next -> next)

    Stream
      .unfoldEval(0L)(next)
      .map(Instant.ofEpochMilli)
  }

  def uniqueInstants[F[_]](implicit F: Sync[F]): Stream[F, Instant] =
    Stream.eval(F.delay(Instant.now())).repeat.changes(Eq.fromUniversalEquals)

}

object Foo extends App {

  val out = Timestamps.uniqueInstants[IO].take(10).compile.toVector.unsafeRunSync()
  out.foreach(println)

}
