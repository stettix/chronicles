package dev.chronicles.cli

trait VersionRepositoryClient[F[_]] {

  def executeAction(action: Action, console: Console[F]): F[Unit]

}

// TODO:
//   - Dummy implementation using memory store
//   - Implementation using REST client
//   Common code for these will live in the trait, and be tested via generic tests for this.
