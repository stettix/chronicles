package dev.chronicles.cli

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import dev.chronicles.cli.StubConsole._

/**
  * Stub implementation of Console, that records interactions with the console.
  */
class StubConsole[F[_]](outputs: Ref[F, List[StubConsole.Event]])(implicit F: Sync[F]) extends Console[F] {

  def output: F[List[StubConsole.Event]] =
    outputs.get.map(_.reverse)

  override def println(msg: String): F[Unit] =
    outputs.modify(prev => (StdOut(msg) :: prev, StdOut(msg))).void

  override def errorln(msg: String): F[Unit] =
    outputs.modify(prev => (StdErr(msg) :: prev, StdErr(msg))).void

  override def readLine(prompt: String): F[Option[String]] =
    outputs
      .modify(prev => (StdIn(prompt) :: prev, StdIn(prompt)))
      .map(_ => None) // Instrumented output from readLine not supported yet.
}

object StubConsole {

  def apply[F[_]](implicit F: Sync[F]): F[StubConsole[F]] =
    Ref[F].of(List[StubConsole.Event]()).map(new StubConsole(_))

  sealed trait Event
  final case class StdOut(line: String) extends Event
  final case class StdErr(line: String) extends Event
  final case class StdIn(prompt: String) extends Event

}
