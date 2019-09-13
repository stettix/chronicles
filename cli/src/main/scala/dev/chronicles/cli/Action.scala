package dev.chronicles.cli

import dev.chronicles.core.TableName
import dev.chronicles.core.VersionTracker.UpdateMessage

sealed trait PartitionOperation

object PartitionOperation {
  case object Add extends PartitionOperation
  case object Remove extends PartitionOperation

  def parse(str: String): Option[PartitionOperation] = str.toLowerCase match {
    case "add"    => Some(PartitionOperation.Add)
    case "remove" => Some(PartitionOperation.Remove)
    case _        => None
  }
}

sealed trait Action

object Action {
  final case object ListTables extends Action
  final case class InitTable(name: TableName, isSnapshot: Boolean, message: UpdateMessage) extends Action
  final case class ShowTableHistory(tableName: TableName) extends Action
  final case class ListPartitions(tableName: TableName) extends Action
  final case class AddPartition(tableName: TableName, partitionName: String, message: UpdateMessage) extends Action
  final case class RemovePartition(tableName: TableName, partitionName: String, message: UpdateMessage) extends Action
}
