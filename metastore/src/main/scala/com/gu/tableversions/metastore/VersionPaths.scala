package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.{Partition, Version}

/**
  * Encodes the mapping between version numbers and storage paths.
  */
object VersionPaths {

  /**
    * @return the fully resolved paths for each partitions, derived from the table location and version
    */
  def resolveVersionedPartitionPaths(
      partitions: List[Partition],
      version: Version,
      tableLocation: URI): Map[Partition, URI] = {

    partitions.map { partition =>
      val partitionBasePath = partition.resolvePath(tableLocation)
      partition -> pathFor(partitionBasePath, version)
    }.toMap
  }

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

}
