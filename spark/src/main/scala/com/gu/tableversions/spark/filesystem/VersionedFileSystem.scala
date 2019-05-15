package com.gu.tableversions.spark.filesystem

import java.net.URI
import java.time.Instant
import java.util.Objects

import cats.syntax.either._
import com.gu.tableversions.core.{Partition, Version}
import com.gu.tableversions.spark.filesystem.VersionedFileSystem.ConfigKeys
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * A Hadoop FileSystem proxy that translates paths according to version configuration.
  *
  * This implementation reads the current version for each partition from a configuration file on initialisation,
  * and creates a PathMapper that performs the translation based on this.
  *
  * Note that paths that don't represent a partition with a specified version will be passed through unchanged; only
  * the scheme of the URI will be converted.
  *
  * This file system uses 'versioned' as its scheme, that is, it will be used for URIs like
  * 'versioned://path/to/data'
  *
  * It is also configured with the scheme of the underlying file system. This is currently set in a global
  * configuration parameter. For example, if this is set to 's3', then the example URI above would be converted
  * to 's3://path/to/data'.
  *
  * Note that for this filesystem to pick up the latest version information, it should run with Hadoop caching
  * disabled, i.e. by setting "fs.versioned.impl.disable.cache" to "true".
  */
class VersionedFileSystem extends ProxyFileSystem with LazyLogging {

  override def initialize(path: URI, conf: Configuration): Unit = {
    logger.info(s"Initialising versioned filesystem with path '$path'")

    val cacheDisabled = conf.getBoolean(ConfigKeys.disableCache, false)
    val baseFsScheme = conf.get(ConfigKeys.baseFS)
    val configDirectory = conf.get(ConfigKeys.configDirectory)

    require(Objects.nonNull(baseFsScheme), s"${ConfigKeys.baseFS} not set in configuration")
    require(Objects.nonNull(configDirectory), s"${ConfigKeys.configDirectory} not set in configuration")
    require(cacheDisabled, s"${ConfigKeys.disableCache} not set to true in configuration")

    logger.info(s"Cache disabled = $cacheDisabled")
    logger.info(s"Base filesystem scheme = $baseFsScheme")
    logger.info(s"Config directory = $configDirectory")

    // When initialising the versioned filesystem we need to swap the versioned:// prefix
    // in the URI passed during initialisation to the base scheme.
    val baseUri = new URI(baseFsScheme, path.getAuthority, path.getPath, path.getQuery, path.getFragment)
    logger.info(s"URI for base filesystem: $baseUri")

    val config = VersionedFileSystem
      .readConfig(new URI(configDirectory), conf)
      .valueOr(e =>
        throw new Exception(s"Unable to read partition version configuration from directory: $configDirectory", e))

    val pathMapper = new VersionedPathMapper(baseFsScheme, config.partitionVersions)

    val baseFs = FileSystem.get(baseUri, conf)
    baseFs.initialize(baseUri, conf)

    initialiseProxyFileSystem(baseUri, baseFs, pathMapper)
  }
}

object VersionedFileSystem extends LazyLogging {

  val scheme = "versioned"

  object ConfigKeys {
    val baseFS = "fs.versioned.baseFS"
    val disableCache = "fs.versioned.impl.disable.cache"
    val configDirectory = "fs.versioned.configDirectory"
  }

  /**
    * Returns Spark config settings needed to enable the VersionedFileSystem.
    *
    * Note that these have the "spark.hadoop." prefix; this means that these parameters will be set
    * in the hadoop configuration of the Spark job, minus the prefix.
    */
  def sparkConfig(baseFileSystemSchema: String, configDirectory: URI): Map[String, String] =
    Map(
      // @formatter: off
      "spark.hadoop.fs.versioned.impl" -> "com.gu.tableversions.spark.filesystem.VersionedFileSystem",
      "spark.hadoop." + ConfigKeys.baseFS -> baseFileSystemSchema,
      "spark.hadoop." + ConfigKeys.configDirectory -> configDirectory.toString,
      "spark.hadoop." + VersionedFileSystem.ConfigKeys.disableCache -> "true"
      // @formatter: on
    )

  private[filesystem] val configEncoding = "UTF-8"
  private[filesystem] val configFilename = "_vfsconfig"

  import cats.syntax.either._
  import io.circe.Decoder._
  import io.circe.generic.auto._
  import io.circe.syntax._

  case class VersionedFileSystemConfig(partitionVersions: Map[Partition, Version])

  private implicit def partitionKeyDecoder: KeyDecoder[Partition] =
    KeyDecoder.instance(s => Partition.parse(s).toOption)

  private implicit def versionDecoder: Decoder[Version] =
    Decoder.decodeString.emap(s => Version.parse(s).leftMap(_.getMessage))

  private implicit def instantDecoder: Decoder[Instant] = Decoder.decodeString.emap { str =>
    Either.catchNonFatal(Instant.parse(str)).leftMap(t => s"Unable to parse instant '$str': " + t.getMessage)
  }

  private implicit def partitionEncoder: KeyEncoder[Partition] =
    KeyEncoder.instance { partition =>
      partition.columnValues.map(cv => s"${cv.column.name}=${cv.value}").toList.mkString("/")
    }

  private implicit def versionEncoder: Encoder[Version] =
    Encoder.encodeString.contramap(_.label)

  /**
    * Write partition version configuration to the given path.
    */
  def writeConfig(config: VersionedFileSystemConfig, hadoopConfiguration: Configuration): Unit = {
    val configDirectoryPath = hadoopConfiguration.get(ConfigKeys.configDirectory)
    val configFile = configFilename(new URI(configDirectoryPath))
    logger.info(s"Writing config with ${config.partitionVersions.size} partition versions to file: $configFile")

    val fs = FileSystem.get(configFile, hadoopConfiguration)
    val os = fs.create(new Path(configFile))
    try {
      val configJson = config.asJson
      val jsonBytes = configJson.noSpaces.getBytes(configEncoding)
      os.write(jsonBytes)
      os.flush()
    } finally {
      os.close()
    }
  }

  /**
    * Read partition version configuration from the given path.
    */
  def readConfig(configDir: URI, hadoopConfiguration: Configuration): Either[Throwable, VersionedFileSystemConfig] = {
    val path = configFilename(configDir)
    logger.info(s"Reading config from path: $path")

    val fs = FileSystem.get(path, hadoopConfiguration)
    for {
      is <- Either.catchNonFatal(fs.open(new Path(path.resolve(VersionedFileSystem.configFilename))))
      configString <- Either.catchNonFatal(IOUtils.toString(is, VersionedFileSystem.configEncoding))
      config <- decode[VersionedFileSystemConfig](configString)
    } yield config
  }

  private[filesystem] def configFilename(configDir: URI): URI = {
    val normalizedConfigDir: URI =
      if (configDir.toString.endsWith("/")) configDir else new URI(configDir.toString + "/")
    normalizedConfigDir.resolve(configFilename)
  }

}
