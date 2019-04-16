package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.{Partition, Version}

/**
  * Encodes the mapping between version numbers and storage paths.
  */
object VersionPaths {

  /**
    * @return the fully resolved path for collection of versioned path
    */
  def resolveVersionedPartitionPaths(
      partitionVersions: Map[Partition, Version],
      tableLocation: URI): Map[Partition, URI] = {

    partitionVersions.map {
      case (partition, version) =>
        val partitionBasePath = partition.resolvePath(tableLocation)
        partition -> pathFor(partitionBasePath, version)
    }
  }

  /**
    * @return a path for a given partition version and base path
    */
  def pathFor(partitionPath: URI, newVersion: Version): URI = {
    def normalised(path: String): String = if (path.endsWith("/")) path else path + "/"
    def versioned(path: String): String = if (newVersion.number <= 0) path else s"${path}v${newVersion.number}"
    new URI(versioned(normalised(partitionPath.toString)))
  }

}
