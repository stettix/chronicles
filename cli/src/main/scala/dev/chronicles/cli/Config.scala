package dev.chronicles.cli

import dev.chronicles.cli.Config.ConnectionParameters
import enumeratum.EnumEntry.Lowercase
import enumeratum._

sealed abstract class DatabaseType extends EnumEntry with Lowercase

object DatabaseType extends Enum[DatabaseType] {

  val values = findValues

  case object Postgresql extends DatabaseType
  case object H2 extends DatabaseType

}

sealed trait Config

final case object MemConfig extends Config

final case class DbConfig(
    dbType: DatabaseType,
    hostname: String,
    port: Int,
    dbName: String,
    username: String,
    password: String)
    extends Config {

  val parameters: ConnectionParameters =
    dbType match {
      case DatabaseType.H2         => ConnectionParameters("org.h2.Driver", s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1")
      case DatabaseType.Postgresql => ConnectionParameters("org.postgresql.Driver", s"jdbc:postgresql:$dbName")
    }

}

object Config {

  case class ConnectionParameters(driverClassName: String, jdbcUrl: String)

  def parseConfig(str: String): Either[Throwable, Config] = Left(new Error("Not implemented yet"))

}
