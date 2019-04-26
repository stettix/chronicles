package com.gu.tableversions.core

import cats.effect.IO
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.TableVersions.TableOperation._
import org.scalatest.{FlatSpec, Matchers}

class InMemoryTableVersionsSpec extends FlatSpec with Matchers with TableVersionsSpec {

  "The reference implementation for the TableVersions service" should behave like tableVersionsBehaviour {
    InMemoryTableVersions[IO]
  }

}
