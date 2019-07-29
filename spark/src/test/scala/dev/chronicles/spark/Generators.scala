package dev.chronicles.spark

import cats.data.NonEmptyList
import cats.effect.IO
import dev.chronicles.core.Partition.{ColumnValue, PartitionColumn}
import dev.chronicles.core.{Partition, Version}
import dev.chronicles.spark.filesystem.VersionedFileSystem.VersionedFileSystemConfig
import org.scalacheck.Gen

trait Generators {

  val nonEmptyStr = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString)

  val columnValueGen: Gen[ColumnValue] =
    nonEmptyStr.flatMap(s => nonEmptyStr.map(t => ColumnValue(PartitionColumn(s), t)))

  val partitionGen: Gen[Partition] = Gen.nonEmptyListOf(columnValueGen).map { columnValues =>
    Partition(NonEmptyList.fromListUnsafe(columnValues))
  }

  val versionGen: Gen[Version] = Version.generateVersion[IO].unsafeRunSync()

  val partitionVersionGen: Gen[(Partition, Version)] = partitionGen.flatMap(p => versionGen.map(v => p -> v))

  val versionedFileSystemConfigGenerator: Gen[VersionedFileSystemConfig] =
    for {
      partitionVersions <- Gen.nonEmptyListOf(partitionVersionGen)
    } yield VersionedFileSystemConfig(partitionVersions.toMap)
}
