package dev.chronicles.filebacked

import java.time.Instant

import dev.chronicles.core.VersionTracker.TableOperation._
import dev.chronicles.core.VersionTracker._
import dev.chronicles.core.{Partition, TableName, Version}
import dev.chronicles.filebacked.FileBackedVersionTracker.{StateFile, TableMetadataFile}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}

object JsonCodecs {

  //
  // TableMetadataFile codecs
  //

  implicit val tableMetadataFileEncoder: Encoder[TableMetadataFile] =
    (t: TableMetadataFile) => Json.obj("is_snapshot" -> t.isSnapshot.asJson)

  implicit val tableMetadataFileDecoder: Decoder[TableMetadataFile] =
    (c: HCursor) =>
      for {
        isSnapshot <- c.downField("is_snapshot").as[Boolean]
      } yield TableMetadataFile(isSnapshot)

  //
  // TableUpdate codecs
  //

  implicit val commitIdEncoder: Encoder[CommitId] = Encoder.encodeString.contramap[CommitId](_.id)
  implicit val userIdEncoder: Encoder[UserId] = Encoder.encodeString.contramap[UserId](_.value)
  implicit val messageEncoder: Encoder[UpdateMessage] = Encoder.encodeString.contramap[UpdateMessage](_.content)
  implicit val versionEncoder: Encoder[Version] = Encoder.encodeString.contramap[Version](_.label)
  implicit val partitionEncoder: Encoder[Partition] = Encoder.encodeString.contramap[Partition](_.toString)

  implicit val tableOperationEncoder: Encoder[TableOperation] = {
    case InitTable(tableName, isSnapshot) =>
      Json.obj(
        "type" -> "init-table".asJson,
        "table_name" -> tableName.fullyQualifiedName.asJson,
        "is_snapshot" -> isSnapshot.asJson
      )
    case TableOperation.AddTableVersion(version) =>
      Json.obj(
        "type" -> "add-table-version".asJson,
        "version" -> version.asJson
      )
    case TableOperation.AddPartitionVersion(partition, version) =>
      Json.obj(
        "type" -> "add-partition-version".asJson,
        "partition" -> partition.asJson,
        "version" -> version.asJson
      )
    case TableOperation.RemovePartition(partition) =>
      Json.obj(
        "type" -> "remove-partition".asJson,
        "partition" -> partition.asJson
      )
  }

  implicit val tableUpdateEncoder: Encoder[TableUpdate] = (t: TableUpdate) =>
    Json.obj(
      "commit_id" -> t.metadata.id.asJson,
      "user_id" -> t.metadata.userId.asJson,
      "message" -> t.metadata.message.asJson,
      "timestamp" -> t.metadata.timestamp.asJson,
      "operations" -> t.operations.asJson
  )

  implicit val commitIdDecoder: Decoder[CommitId] = Decoder.decodeString.map(CommitId)
  implicit val userIdDecoder: Decoder[UserId] = Decoder.decodeString.map(UserId)
  implicit val commitMessageDecoder: Decoder[UpdateMessage] = Decoder.decodeString.map(UpdateMessage)
  implicit val tableNameDecoder: Decoder[TableName] =
    Decoder.decodeString.emap(TableName.fromFullyQualifiedName(_).left.map(_.getMessage))
  implicit val versionDecoder: Decoder[Version] =
    Decoder.decodeString.emap(Version.parse(_).left.map(_.getMessage))
  implicit val partitionDecoder: Decoder[Partition] =
    Decoder.decodeString.emap(Partition.parse(_).left.map(_.getMessage))

  implicit val tableOperationDecoder: Decoder[TableOperation] =
    (c: HCursor) =>
      c.get[String]("type").flatMap {
        case "init-table" =>
          for {
            tableName <- c.downField("table_name").as[TableName]
            isSnapshot <- c.downField("is_snapshot").as[Boolean]
          } yield InitTable(tableName, isSnapshot)

        case "add-table-version" =>
          for {
            version <- c.downField("version").as[Version]
          } yield AddTableVersion(version)

        case "add-partition-version" =>
          for {
            partition <- c.downField("partition").as[Partition]
            version <- c.downField("version").as[Version]
          } yield AddPartitionVersion(partition, version)

        case "remove-partition" =>
          for {
            partition <- c.downField("partition").as[Partition]
          } yield RemovePartition(partition)

        case t => Decoder.failedWithMessage(s"Unexpected value for 'type' field: $t")(c)
    }

  implicit val tableUpdateDecoder: Decoder[TableUpdate] =
    (c: HCursor) =>
      for {
        commitId <- c.downField("commit_id").as[CommitId]
        userId <- c.downField("user_id").as[UserId]
        message <- c.downField("message").as[UpdateMessage]
        timestamp <- c.downField("timestamp").as[Instant]
        operations <- c.downField("operations").as[List[TableOperation]]
      } yield TableUpdate(TableUpdateMetadata(commitId, userId, message, timestamp), operations)

  /** Defines the format to use for rendering all JSON documents. */
  def renderJson[T](x: T)(implicit T: Encoder[T]): String =
    x.asJson.spaces2

  //
  // StateFile codecs
  //

  implicit val stateFileEncoder: Encoder[StateFile] =
    (s: StateFile) => Json.obj("head_ref" -> s.headRef.asJson)

  implicit val stateFileDecoder: Decoder[StateFile] =
    (c: HCursor) =>
      for {
        headRef <- c.downField("head_ref").as[String]
      } yield StateFile(headRef)

}
