package dev.chronicles.core

import java.net.URI

import cats.effect.IO
import org.scalatest.{FlatSpec, Matchers}

class VersionPathsSpec extends FlatSpec with Matchers {

  val version = Version.generateVersion[IO].unsafeRunSync()

  "Resolving path with a version" should "have the version string appended as a folder" in {
    VersionPaths.pathFor(new URI("s3://foo/bar/"), version) shouldBe new URI(s"s3://foo/bar/${version.label}")
  }

  it should "return a normalised path if given a path that doesn't end in a '/'" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), version) shouldBe new URI(s"s3://foo/bar/${version.label}")
  }

  it should "return the original path if no actual version is given" in {
    VersionPaths.pathFor(new URI("s3://foo/bar"), Version.Unversioned) shouldBe new URI("s3://foo/bar")
  }

  "Parsing the version from versioned paths" should "produce the version number" in {
    VersionPaths.parseVersion(new URI(s"file:/tmp/7bbc577c-471d-4ece-8462/table/date=2019-01-21/2019/${version.label}")) shouldBe version

    VersionPaths.parseVersion(new URI(s"s3://bucket/pageview/date=2019-01-21/${version.label}")) shouldBe version
    VersionPaths.parseVersion(new URI(s"s3://bucket/identity/${version.label}")) shouldBe version
  }

  "Parsing the version from unversioned paths" should "produce 'Unversioned' version" in {
    VersionPaths.parseVersion(new URI("s3://bucket/pageview/date=2019-01-21")) shouldBe Version.Unversioned
    VersionPaths.parseVersion(new URI("s3://bucket/identity")) shouldBe Version.Unversioned
  }

  "Getting the base path" should "return the same path if it's already unversioned" in {
    VersionPaths.versionedToBasePath(new URI("hdfs://bucket/identity")) shouldBe new URI("hdfs://bucket/identity")
  }

  it should "return strip off the version part of the path" in {
    VersionPaths.versionedToBasePath(new URI(s"hdfs://bucket/identity/${version.label}")) shouldBe new URI(
      "hdfs://bucket/identity")
  }

}
