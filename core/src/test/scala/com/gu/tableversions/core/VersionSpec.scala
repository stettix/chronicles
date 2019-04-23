package com.gu.tableversions.core

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID

import cats.implicits._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class VersionSpec extends FlatSpec with Matchers with EitherValues with PropertyChecks {

  "Generating versions" should "produce valid labels" in {
    val version = Version.generateVersion.unsafeRunSync()
    version.label should fullyMatch regex Version.TimestampAndUuidRegex
  }

  it should "produce unique labels" in {
    val versions = (1 to 100).map(_ => Version.generateVersion).toList.sequence.unsafeRunSync()
    versions.distinct.size shouldBe versions.size
  }

  "The label format regex" should "match valid labels" in {
    val validVersionLabel = "20181102-235900.123456789-4920d06f-2233-4b4a-9521-8e730eee89c5"
    validVersionLabel should fullyMatch regex Version.TimestampAndUuidRegex
  }

  "Version.parse" should "parse a valid version label" in {
    val validVersionLabel = "20181102-235912.987654321-4920d06f-2233-4b4a-9521-8e730eee89c5"

    val expectedTimestamp = LocalDateTime.of(2018, 11, 2, 23, 59, 12, 987654321).atZone(ZoneId.of("UTC"))
    val expectedInstant = Instant.from(expectedTimestamp)
    val expectedUuId = UUID.fromString("4920d06f-2233-4b4a-9521-8e730eee89c5")
    Version.parse(validVersionLabel) shouldBe Right(Version(expectedInstant, expectedUuId))
  }

  it should "return an error for invalid version label" in {
    val error = Version.parse("invalidLabel").left.value
    error.getMessage shouldBe "invalid version label format invalidLabel"
  }

  it should "return an error for a label with an invalid UUID part" in {
    val invalidLabel = "20181102-235900-foobar"
    val error = Version.parse(invalidLabel).left.value
    error.getMessage.toLowerCase should include regex s"invalid.*foobar"
  }

  it should "get the same Version back after rendering and parsing it" in {
    val genVersion = Gen.delay(Version.generateVersion.unsafeRunSync())
    forAll(genVersion) { version =>
      Version.parse(version.label) shouldBe Right(version)
    }
  }

}
