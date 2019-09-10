package dev.chronicles.cli

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
  final case class ShowTableHistory(tableName: String) extends Action
  final case class ListPartitions(tableName: String) extends Action
  final case class AddPartition(tableName: String, partitionName: String) extends Action
  final case class RemovePartition(tableName: String, partitionName: String) extends Action
}
