package dev.chronicles.cli

import dev.chronicles.core.TableName
import dev.chronicles.core.VersionTracker.{UpdateMessage, UserId}

sealed trait PartitionOperation

object PartitionOperation {
  case object Add extends PartitionOperation
  case object Remove extends PartitionOperation

  def parse(str: String): Option[PartitionOperation] = str match {
    case "add"    => Some(PartitionOperation.Add)
    case "remove" => Some(PartitionOperation.Remove)
    case _        => None
  }
}

sealed trait Action

object Action {
  final case object ListTables extends Action
  final case class InitTable(name: TableName, isSnapshot: Boolean, userId: UserId, message: UpdateMessage)
      extends Action
  final case class ShowTableHistory(tableName: TableName) extends Action
  final case class ListPartitions(tableName: TableName) extends Action
  final case class AddPartition(tableName: TableName, partitionName: String) extends Action
  final case class RemovePartition(tableName: TableName, partitionName: String) extends Action
}
