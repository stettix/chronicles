package com.gu.tableversions.core

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.UUID

import cats.effect.IO
import cats.syntax.either._

import scala.util.Try

/**
  * Type that represents a valid version, as used by tables and partitions.
  */
final case class Version(timestamp: Instant, uuid: UUID) {

  import Version._

  def label: String = {
    val versionString = uuid
    val localDateTime = LocalDateTime.ofInstant(timestamp, ZoneId.of("UTC"))
    val timestampStr = timestampFormatter.format(localDateTime)
    s"$timestampStr-$versionString"
  }

}

object Version {

  val Unversioned = Version(Instant.MIN, new UUID(0L, 0L))

  /** Generator for versions using a timestamp + UUID format */
  implicit val generateVersion: IO[Version] =
    IO { Version(Instant.now(), UUID.randomUUID()) }

  /** Regex that can be used to recognise a valid format string. */
  val TimestampAndUuidRegex = """(\d{8}-\d{6}.\d{9})-(.*)""".r

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.nnnnnnnnn")

  /**
    * Try to parse a Version object from a string.
    */
  def parse(label: String): Either[Throwable, Version] = {

    def parseVersionFields(label: String): Either[Throwable, (String, String)] = label match {
      case Version.TimestampAndUuidRegex(timestampStr, uuidStr) => Right((timestampStr, uuidStr))
      case _                                                    => Left(new IllegalArgumentException(s"invalid version label format $label"))
    }

    for {
      versionFields <- parseVersionFields(label)
      (timestampStr, uuidStr) = versionFields
      uuid <- Either.fromTry(Try(UUID.fromString(uuidStr)))
      zonedDateTime = LocalDateTime.parse(timestampStr, Version.timestampFormatter).atZone(ZoneId.of("UTC"))
      version = Version(Instant.from(zonedDateTime), uuid)
    } yield version
  }

}
