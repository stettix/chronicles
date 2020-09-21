package dev.chronicles.filebacked

import io.circe.Codec
import io.circe.generic.semiauto._

object JsonSchemas {

  case class TableMetadataFile(isSnapshot: Boolean)

  implicit val tableMetadataFileCodec: Codec[TableMetadataFile] = deriveCodec[TableMetadataFile]

}
