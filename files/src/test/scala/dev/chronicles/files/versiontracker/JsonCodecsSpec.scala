package dev.chronicles.files.versiontracker

import java.time.Instant
import java.util.UUID

import dev.chronicles.core.Partition.{ColumnValue, PartitionColumn}
import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core.{Partition, TableName, Version}
import FileBackedVersionTracker.{StateFile, TableMetadataFile}
import JsonCodecs._
import io.circe.parser._
import org.scalatest.{FlatSpec, Matchers}

class JsonCodecsSpec extends FlatSpec with Matchers {

  val time = Instant.parse("2021-12-03T10:15:30.01Z")
  val tableVersion = Version(Instant.now(), UUID.randomUUID())
  val partitionVersion = Version(Instant.now(), UUID.randomUUID())

  val sampleTableUpdate = TableUpdate(
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

  "Round trip coding and decoding of a table update" should "produce the same value" in {
    val decoded = decode[TableUpdate](JsonCodecs.renderJson(sampleTableUpdate))
    decoded shouldBe Right(sampleTableUpdate)
  }

  "The rendered JSON for a table update" should "have the required format" in {

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

    JsonCodecs.renderJson(sampleTableUpdate) shouldBe expectedJson
  }

  val sampleTableMetadataFile = TableMetadataFile(isSnapshot = true)

  "The rendered JSON for a table metadata file" should "have the required format" in {
    val expectedJson =
      s"""{
         |  "is_snapshot" : true
         |}""".stripMargin

    JsonCodecs.renderJson(sampleTableMetadataFile) shouldBe expectedJson
  }

  it should "be read back to the original value" in {
    val decoded = decode[TableMetadataFile](JsonCodecs.renderJson(sampleTableMetadataFile))
    decoded shouldBe Right(sampleTableMetadataFile)
  }

  val sampleStateFile = StateFile(headRef = "xyz")

  "The rendered JSON for a state file" should "have the required format" in {
    val expectedJson =
      s"""{
         |  "head_ref" : "xyz"
         |}""".stripMargin

    JsonCodecs.renderJson(sampleStateFile) shouldBe expectedJson
  }

  it should "be read back to the original value" in {
    val decoded = decode[StateFile](JsonCodecs.renderJson(sampleStateFile))
    decoded shouldBe Right(sampleStateFile)
  }

}
