package dev.chronicles.cli

case class Config()

object Config {

  def parseConfig(str: String): Either[Throwable, Config] = ???

}
