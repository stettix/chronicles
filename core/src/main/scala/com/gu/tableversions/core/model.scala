package com.gu.tableversions.core

//
// A partition schema describes the fields used for partitions of a table
//
case class PartitionColumn(name: String) extends AnyVal

case class PartitionSchema(keys: List[PartitionColumn])
