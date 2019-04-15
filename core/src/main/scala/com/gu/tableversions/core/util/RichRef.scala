package com.gu.tableversions.core.util

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._

object RichRef {

  implicit class RichRef[F[_], A](underlying: Ref[F, A])(implicit F: Sync[F]) {

    /**
      * Perform a conditional update on the `Ref` that produces a failure for an invalid update.
      *
      * If the provided update function `f` returns a `Right[A]`, the contained value will be used to update the `Ref`.
      * If `f` returns a `Left[E]`, this will cause the return `F` to be an error.
      */
    def modifyEither[E <: Throwable](f: A => Either[E, A]): F[Unit] =
      underlying
        .modify(a =>
          f(a) match {
            case Left(e)     => (a, e.raiseError[F, Unit])
            case Right(newA) => (newA, F.unit)
        })
        .flatten
  }

}
