package com.gu.tableversions.spark.filesystem

import cats.implicits._
import com.gu.tableversions.core.{Partition, Version}
import org.apache.hadoop.fs.Path
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FreeSpec, Matchers}

class VersionedPathMapperSpec extends FreeSpec with Matchers with TableDrivenPropertyChecks {

  "VersionedPathMapper" - {
    val v1 = Version.generateVersion.unsafeRunSync()
    val v2 = Version.generateVersion.unsafeRunSync()
    val v3 = Version.generateVersion.unsafeRunSync()

    val partitionVersions: Map[Partition, Version] = Map(
      partition("date=2019-01-01") -> v1,
      partition("date=2019-01-02") -> v2,
      partition("date=2019-01-03") -> v3,
      partition("date=2019-01-15") -> v1,
      partition("received_date=2019-01-03/processed_date=2019-01-04") -> v2,
      partition("year=2019/month=jan/day=01") -> v2,
      partition("xyz_foo_baz_42=42/bar=2019-01-03") -> v1
    )

    val modifier = new VersionedPathMapper("s3", partitionVersions)

    "for paths defined in the mapping" - {
      // @formatter:off
      val partitionMappingTable = Table(
        ("Unversioned partition", "Versioned partition"),

        // Single partition column
        ("versioned://some-bucket/date=2019-01-01", s"s3://some-bucket/date=2019-01-01/${v1.label}"),
        ("versioned://some-bucket/date=2019-01-01/", s"s3://some-bucket/date=2019-01-01/${v1.label}/"),
        ("versioned://some-bucket/some/path/to/table/date=2019-01-01", s"s3://some-bucket/some/path/to/table/date=2019-01-01/${v1.label}"),
        ("versioned://some-bucket/date=2019-01-03", s"s3://some-bucket/date=2019-01-03/${v3.label}"),

        // Multiple partition columns
        ("versioned://some-bucket/received_date=2019-01-03/processed_date=2019-01-04", s"s3://some-bucket/received_date=2019-01-03/processed_date=2019-01-04/${v2.label}"),
        ("versioned://some-bucket/year=2019/month=jan/day=01", s"s3://some-bucket/year=2019/month=jan/day=01/${v2.label}"),
        ("versioned://some-bucket/xyz_foo_baz_42=42/bar=2019-01-03", s"s3://some-bucket/xyz_foo_baz_42=42/bar=2019-01-03/${v1.label}"),

        // Including file name
        ("versioned://some-bucket/date=2019-01-01/file.parquet", s"s3://some-bucket/date=2019-01-01/${v1.label}/file.parquet"),
        ("versioned://some-bucket/date=2019-01-03/some/long/directory/path/file.parquet", s"s3://some-bucket/date=2019-01-03/${v3.label}/some/long/directory/path/file.parquet"),

        // Nested directory inside versioned partition
        ("versioned://some-bucket/date=2019-01-01/temp/file.parquet", s"s3://some-bucket/date=2019-01-01/${v1.label}/temp/file.parquet"),

        // Something that looks like a partition folder in the root part of the table
        ("versioned://some-bucket/some=yes/path/to/table/date=2019-01-01", s"s3://some-bucket/some=yes/path/to/table/date=2019-01-01/${v1.label}"),

        // Various valid syntax examples
        ("versioned:/some-bucket/dir/date=2019-01-01", s"s3:/some-bucket/dir/date=2019-01-01/${v1.label}"),
        ("versioned:///some-bucket/dir/date=2019-01-01", s"s3:///some-bucket/dir/date=2019-01-01/${v1.label}")
      )
      // @formatter:on

      "appends versions to input Paths and sets the scheme to the underlying FS" in
        forAll(partitionMappingTable) {
          case (versionedUri, s3Uri) =>
            modifier.forUnderlying(new Path(versionedUri)) should equal(new Path(s3Uri))
        }

      "removes versions from output Paths and sets the scheme to versioned://" in
        forAll(partitionMappingTable) {
          case (versionedUri, s3Uri) =>
            modifier.fromUnderlying(new Path(s3Uri)) should equal(new Path(versionedUri))
        }
    }

    "for paths not defined in the mapping" - {
      "it should only change the scheme when converting path to the underlying filesystem" in {
        modifier.fromUnderlying(new Path("s3://some-bucket/date=2019-01-04")) shouldBe new Path(
          "versioned://some-bucket/date=2019-01-04")
      }

      "it should only change the scheme when converting path from the underlying filesystem" in {
        modifier.forUnderlying(new Path("versioned://some-bucket/date=2019-01-04")) shouldBe new Path(
          "s3://some-bucket/date=2019-01-04")
      }
    }

  }

  private def partition(str: String): Partition = Partition.parse(str).valueOr(throw _)

}
