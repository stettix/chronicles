package dev.chronicles.files.versiontracker

import java.time.Instant

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

/**
  * Implements a monotonic, effectful clock that provides timestamps with millisecond resolution that are guaranteed to be unique.
  */
class MonotonicClock[F[_]] private (last: Ref[F, Option[Instant]])(implicit F: Sync[F]) {

  val nextTimestamp: F[Instant] = for {
    now <- F.delay(Instant.now())
    timestamp <- last.modify { prevTimestamp =>
      if (prevTimestamp.exists(prev => !now.isAfter(prev)))
        (prevTimestamp.map(_.plusMillis(1)), now)
      else
        (Option(now), now)
    }
  } yield timestamp

}

object MonotonicClock {

  def apply[F[_]](implicit F: Sync[F]): F[MonotonicClock[F]] = {
    Ref.of[F, Option[Instant]](None).map(ref => new MonotonicClock(ref))
  }

}
