package com.gu.tableversions.examples

import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

object DateTime {

  val Utc: ZoneId = ZoneId.of("UTC")

  def timestampToUtcDate(timestamp: Timestamp): Date = {
    val zonedDateTime = ZonedDateTime.ofInstant(timestamp.toInstant, Utc)
    java.sql.Date.valueOf(zonedDateTime.toLocalDate)
  }

}
