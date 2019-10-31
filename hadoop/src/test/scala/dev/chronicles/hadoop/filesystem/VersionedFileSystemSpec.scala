package dev.chronicles.hadoop.filesystem

import java.net.URI
import java.nio.file.Files

import dev.chronicles.hadoop.filesystem.VersionedFileSystem.ConfigKeys
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{EitherValues, FreeSpec, Matchers}

import scala.util.Try

class VersionedFileSystemSpec
    extends FreeSpec
    with Matchers
    with EitherValues
    with GeneratorDrivenPropertyChecks
    with Generators {

  "Written config files should be parsed successfully" in forAll(versionedFileSystemConfigGenerator) { versionConf =>
    val tmpDir = Files.createTempDirectory(getClass.getSimpleName)

    try {
      val uri = tmpDir.toUri
      val hadoopConfiguration: Configuration = new Configuration()
      hadoopConfiguration.set(ConfigKeys.configDirectory, uri.toString)

      VersionedFileSystem.writeConfig(versionConf, hadoopConfiguration)
      val read = VersionedFileSystem.readConfig(uri, hadoopConfiguration)
      read.right.value shouldBe versionConf
    } finally {
      Try(FileUtils.deleteDirectory(tmpDir.toFile))
      ()
    }
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
