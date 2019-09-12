package dev.chronicles.cli

import java.time.Instant

import cats.effect.Sync

/** Simple effectful clock */
trait Clock[F[_]] {
  val now: F[Instant]
}

class JavaSystemClock[F[_]](implicit F: Sync[F]) extends Clock[F] {
  override val now: F[Instant] = F.delay(Instant.now())
}
