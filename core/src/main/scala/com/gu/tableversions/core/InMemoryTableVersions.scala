package com.gu.tableversions.core

/**
  * Reference implementation of the table version store. Does not persist state.
  */
class InMemoryTableVersions[F[_]] extends TableVersions[F] {

  override def init(table: TableName): F[Unit] = ???

  override def currentVersion(table: TableName): F[TableVersion] = ???

  override def nextVersions(table: TableName, partitionColumns: List[Partition]): F[List[PartitionVersion]] = ???

  override def commit(newVersion: TableVersions.TableUpdate): F[TableVersions.CommitResult] = ???

}
