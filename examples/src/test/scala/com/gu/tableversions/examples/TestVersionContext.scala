package com.gu.tableversions.examples

import cats.effect.IO
import com.gu.tableversions.core.{InMemoryTableVersions, Version, VersionedMetastore}
import com.gu.tableversions.spark.{SparkHiveMetastore, VersionContext}
import org.apache.spark.sql.SparkSession

object TestVersionContext {

  def default(implicit spark: SparkSession): IO[VersionContext] =
    for {
      tableVersions <- InMemoryTableVersions[IO]
      metastore = new SparkHiveMetastore[IO]()
      versionGenerator = Version.generateVersion
    } yield VersionContext(VersionedMetastore(tableVersions, metastore), versionGenerator)
}
