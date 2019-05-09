package com.gu.tableversions.spark.filesystem

import java.net.URI

import com.gu.tableversions.spark.{Generators, SparkHiveSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{EitherValues, FreeSpec, Matchers}

class VersionedFileSystemSpec
    extends FreeSpec
    with Matchers
    with EitherValues
    with GeneratorDrivenPropertyChecks
    with Generators
    with SparkHiveSuite {

  override def customConfig = VersionedFileSystem.sparkConfig("file", tableDir.toUri)

  "Written config files should be parsed successfully" in forAll(versionedFileSystemConfigGenerator) { conf =>
    VersionedFileSystem.writeConfig(conf, spark.sparkContext.hadoopConfiguration)
    val read = VersionedFileSystem.readConfig(tableUri, spark.sparkContext.hadoopConfiguration)
    read.right.value shouldBe conf
  }

  "Resolving config file name" - {

    "should resolve correctly if given directory ends in a slash" in {
      VersionedFileSystem.configFilename(new URI("s3://foo/bar/")) shouldBe new URI(
        s"s3://foo/bar/${VersionedFileSystem.configFilename}")
    }

    "should resolve correctly if given directory does not end in a slash" in {
      VersionedFileSystem.configFilename(new URI("s3://foo/bar")) shouldBe new URI(
        s"s3://foo/bar/${VersionedFileSystem.configFilename}")
    }

  }
}
