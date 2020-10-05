package dev.chronicles.filebacked

import java.time.Instant
import java.util.UUID

import dev.chronicles.core.Partition.{ColumnValue, PartitionColumn}
import dev.chronicles.core.{Partition, TableName, Version}
import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.filebacked.JsonCodecs._
import io.circe.parser._
import io.circe.syntax.EncoderOps
import org.scalatest.{FlatSpec, Matchers}

class JsonCodecsSpec extends FlatSpec with Matchers {

  val time = Instant.parse("2021-12-03T10:15:30.01Z")
  val tableVersion = Version(Instant.now(), UUID.randomUUID())
  val partitionVersion = Version(Instant.now(), UUID.randomUUID())

  val sample = TableUpdate(
    TableUpdateMetadata(CommitId("update-id"), UserId("user ID"), UpdateMessage("update message"), timestamp = time),
    List(
      InitTable(TableName("schema", "table"), isSnapshot = true),
      AddTableVersion(tableVersion),
      AddPartitionVersion(Partition(ColumnValue(PartitionColumn("year"), "2020"),
                                    ColumnValue(PartitionColumn("month"), "12")),
                          partitionVersion),
      RemovePartition(Partition(ColumnValue(PartitionColumn("year"), "2020")))
    )
  )

  // TODO:
  //   - Add generic roundtrip tests
  //   - Test the table metadata file JSON too
  //   - Rename JsonSchemas to JsonCodecs

  "Round trip coding and decoding of a table update" should "produce the same value" in {
    val decoded = decode[TableUpdate](JsonCodecs.renderJson(sample))
    decoded shouldBe Right(sample)
  }

  "The rendered JSON" should "use snake case fields and include custom type discriminator field" in {

    val expectedJson =
      s"""{
        |  "commit_id" : "update-id",
        |  "user_id" : "user ID",
        |  "message" : "update message",
        |  "timestamp" : "2021-12-03T10:15:30.010Z",
        |  "operations" : [
        |    {
        |      "type" : "init-table",
        |      "table_name" : "schema.table",
        |      "is_snapshot" : true
        |    },
        |    {
        |      "type" : "add-table-version",
        |      "version" : "$tableVersion"
        |    },
        |    {
        |      "type" : "add-partition-version",
        |      "partition" : "year=2020/month=12",
        |      "version" : "$partitionVersion"
        |    },
        |    {
        |      "type" : "remove-partition",
        |      "partition" : "year=2020"
        |    }
        |  ]
        |}""".stripMargin

    JsonCodecs.renderJson(sample) shouldBe expectedJson
  }

}
