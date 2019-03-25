package com.gu.tableversions.metastore

import com.gu.tableversions.core.{TableName, TableVersion}

/**
  * Concrete implementation of the Metastore API, using the Hive Client API.
  */
class HiveMetastore[F[_]] extends Metastore[F] {

  override def currentVersion(table: TableName): F[Option[TableVersion]] = ???

  override def update(table: TableName, changes: Metastore.TableChanges): F[Unit] = ???
}
