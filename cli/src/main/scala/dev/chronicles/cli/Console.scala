package dev.chronicles.cli

import cats.effect._
import fs2.Stream

import scala.io.StdIn

/**
  * A simple pure functional console.
  */
trait Console[F[_]] {
  def println(msg: String): F[Unit]
  def errorln(msg: String): F[Unit]
  def readLine(prompt: String): F[Option[String]]

  final def printlns(msgs: Stream[F, String]): Stream[F, Unit] =
    msgs.evalMap(println)
}

object Console {

  /** Create console implementation that uses stdin/stdout. */
  def apply[F[_]: Sync: ContextShift]: F[Console[F]] =
    Sync[F].delay {
      new Console[F] {
        def println(msg: String): F[Unit] =
          Sync[F].delay(System.out.println(msg))

        def errorln(msg: String): F[Unit] =
          Sync[F].delay(System.err.println(msg))

        def readLine(prompt: String): F[Option[String]] =
          Sync[F]
            .delay(Option(StdIn.readLine(prompt)): Option[String])

      }
    }

}
