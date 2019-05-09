package com.gu.tableversions.metastore

import java.io.File
import java.net.URI
import cats.implicits._

import com.gu.tableversions.core.{Partition, Version}

/**
  * Encodes the mapping between version numbers and storage paths.
  */
object VersionPaths {

  /**
    * @return a path for a given partition version and base path
    */
  def pathFor(partitionPath: URI, version: Version): URI =
    if (version == Version.Unversioned)
      partitionPath
    else {
      def normalised(path: String): String = if (path.endsWith("/")) path else path + "/"
      def versioned(path: String): String = s"$path${version.label}"
      new URI(versioned(normalised(partitionPath.toString)))
    }

  /**
    *
    * @return the corresponding version to the provided location
    */
  def parseVersion(location: URI): Version = {
    val maybeVersionStr: Option[String] = location.toString.split("/").lastOption
    val parsedVersion = for {
      path <- maybeVersionStr.toRight(new Exception(s"Empty path: $location"))
      version <- Version.parse(path)
    } yield version

    parsedVersion.getOrElse(Version.Unversioned)
  }

  /**
    *
    * @return the provided location with the version part removed (if present)
    */
  def versionedToBasePath(location: URI): URI = {
    def parentPath(uri: URI): URI = {
      val parentPath = new File(uri.getPath).getParent
      new URI(uri.getScheme, uri.getUserInfo, uri.getHost, uri.getPort, parentPath, uri.getQuery, uri.getFragment)
    }

    if (parseVersion(location) == Version.Unversioned)
      location
    else
      parentPath(location)

  }

}
