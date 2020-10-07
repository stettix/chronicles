package dev.chronicles.files.versiontracker

import java.time.Instant

import cats.Eq
import cats.effect._
import fs2.Stream

object Timestamps {

  /**
    * Produces a stream of timestamps that are guaranteed to be greater than or equal to previous values in the stream.
    */
  def uniqueTimestamps[F[_]](implicit F: Sync[F]): Stream[F, Instant] =
    Stream.repeatEval(F.delay(Instant.now())).changes(Eq.fromUniversalEquals)

}
