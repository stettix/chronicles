package dev.chronicles.examples

import cats.effect.IO
import dev.chronicles.core.{InMemoryTableVersions, Version, VersionedMetastore}
import dev.chronicles.spark.SparkHiveMetastore
import dev.chronicles.spark.{SparkHiveMetastore, VersionContext}
import org.apache.spark.sql.SparkSession

object TestVersionContext {

  def default(implicit spark: SparkSession): IO[VersionContext[IO]] =
    for {
      tableVersions <- InMemoryTableVersions[IO]
      metastore = new SparkHiveMetastore[IO]()
      versionGenerator = Version.generateVersion[IO]
    } yield VersionContext(VersionedMetastore(tableVersions, metastore), versionGenerator)
}
