package dev.chronicles.acceptancetests

import java.time.Instant

import cats.effect.IO
import dev.chronicles.core.TableDefinition
import dev.chronicles.core.VersionTracker.{UpdateMessage, UserId}
import com.typesafe.scalalogging.LazyLogging
import dev.chronicles.spark.{SparkSupport, VersionContext}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
  * Helper code for loading data into versioned tables and updating the versions of such tables.
  */
class TableLoader[T <: Product: TypeTag](
    versionContext: VersionContext[IO],
    table: TableDefinition,
    createTableDdl: String,
    isSnapshot: Boolean)(implicit spark: SparkSession)
    extends LazyLogging {

  import spark.implicits._

  def data(): Dataset[T] =
    spark.table(table.name.fullyQualifiedName).as[T]

}
