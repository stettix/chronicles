package com.gu.tableversions.metastore

import java.net.URI

import com.gu.tableversions.core.{Partition, PartitionVersion, VersionNumber}

/**
  * Encodes the mapping between version numbers and storage paths.
  */
object VersionPaths {

  /**
    * @return the fully resolved path for collection of versioned path
    */
  def resolveVersionedPartitionPaths(
      partitionVersions: List[PartitionVersion],
      tableLocation: URI): Map[Partition, URI] = {

    partitionVersions.map { partitionVersion =>
      val versionByPartition: Map[Partition, VersionNumber] = partitionVersions.map(v => v.partition -> v.version).toMap
      val partitionBasePath = partitionVersion.partition.resolvePath(tableLocation)
      partitionVersion.partition -> pathFor(partitionBasePath, versionByPartition(partitionVersion.partition))
    }.toMap
  }

  /**
    * @return a path for a given partition version and base path
    */
  def pathFor(partitionPath: URI, newVersion: VersionNumber): URI = {
    def normalised(path: String): String = if (path.endsWith("/")) path else path + "/"
    def versioned(path: String): String = if (newVersion.number <= 0) path else s"${path}v${newVersion.number}"
    new URI(versioned(normalised(partitionPath.toString)))
  }

}
