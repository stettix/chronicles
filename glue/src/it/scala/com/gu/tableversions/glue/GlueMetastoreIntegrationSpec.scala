package com.gu.tableversions.glue

import java.net.URI

import cats.effect.IO
import cats.implicits._
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.glue.model._
import com.amazonaws.services.glue.{AWSGlue, AWSGlueClient}
import com.gu.tableversions.core.Partition.PartitionColumn
import com.gu.tableversions.core.{MetastoreSpec, _}
import org.scalatest.{Assertion, FlatSpec, Matchers}

import scala.util.{Properties, Random}

class GlueMetastoreIntegrationSpec extends FlatSpec with Matchers with MetastoreSpec {

  def readMandatoryEnvVariable(varName: String): Either[String, String] =
    Properties.envOrNone(varName).toRight(s"$varName environment variable must be set")

  val AWSProfileEnvVarName = "TABLE_VERSIONS_TEST_AWS_PROFILE"
  val AWSRegionEnvVarName = "TABLE_VERSIONS_TEST_AWS_REGION"
  val SchemaEnvVarName = "TABLE_VERSIONS_TEST_GLUE_DATABASE"

  val envVars = for {
    schema <- readMandatoryEnvVariable(SchemaEnvVarName)
    awsRegion <- readMandatoryEnvVariable(AWSRegionEnvVarName)
    awsProfile = Properties.envOrNone(AWSProfileEnvVarName)
  } yield (schema, awsRegion, awsProfile)

  envVars match {
    case Left(error)                                 => cancel(error)
    case Right((schema, awsRegion, maybeAwsProfile)) => runWithVariables(schema, awsRegion, maybeAwsProfile)
  }

  def runWithVariables(schema: String, awsRegion: String, awsProfile: Option[String]): Unit = {

    val providers: List[AWSCredentialsProvider] = {

      List(new EnvironmentVariableCredentialsProvider, new SystemPropertiesCredentialsProvider) ++
        awsProfile.map(new ProfileCredentialsProvider(_)).toList ++
        List(new ProfileCredentialsProvider, new InstanceProfileCredentialsProvider(false))

    }
    lazy val credentials = new AWSCredentialsProviderChain(providers: _*)

    val glue: AWSGlue = AWSGlueClient.builder().withCredentials(credentials).withRegion(awsRegion).build()

    val tableLocation = new URI("/table-versions-test/")
    val testInputFormat = "org.fake.InputFormat"
    val testOutPutFormat = "org.fake.OutputFormat"
    val testSerialisationLib = "org.fake.serde.FakeSerde"

    val dedupSuffix = Random.alphanumeric.take(8).mkString("")

    val snapshotTable = {
      val tableName = "test_snapshot_" + dedupSuffix
      TableDefinition(TableName(schema, tableName), tableLocation, PartitionSchema.snapshot, FileFormat.Parquet)
    }

    val partitionedTable = {
      val tableName = "test_partitioned_" + dedupSuffix

      TableDefinition(TableName(schema, tableName),
                      tableLocation,
                      PartitionSchema(List(PartitionColumn("date"))),
                      FileFormat.Parquet)
    }

    "A metastore implementation" should behave like metastoreWithSnapshotSupport(IO {
      new GlueMetastore(glue)
    }, initTable(snapshotTable), snapshotTable.name, deleteTable(snapshotTable.name))

    it should behave like metastoreWithPartitionsSupport(IO {
      new GlueMetastore(glue)
    }, initTable(partitionedTable), partitionedTable.name, deleteTable(partitionedTable.name))

    def initTable(table: TableDefinition): IO[Unit] = {
      val serdeInfo =
        new SerDeInfo().withSerializationLibrary(testSerialisationLib)
      val storageDescription = new StorageDescriptor()
        .withLocation(table.location.toString)
        .withColumns(
          new Column().withName("id").withType("String"),
          new Column().withName("field1").withType("String")
        )
        .withSerdeInfo(serdeInfo)
        .withInputFormat(testInputFormat)
        .withOutputFormat(testOutPutFormat)

      val input = {
        val unpartitionedInput = new TableInput()
          .withName(table.name.name)
          .withDescription("table used in integration tests for table versions")
          .withStorageDescriptor(storageDescription)

        if (table.isSnapshot)
          unpartitionedInput
        else
          unpartitionedInput.withPartitionKeys(new Column().withName("date").withType("date"))
      }

      val req = new CreateTableRequest().withDatabaseName(table.name.schema).withTableInput(input)
      IO {
        glue.createTable(req)
      }.void
    }

    def deleteTable(tableName: TableName): IO[Unit] = {
      val deleteRequest = new DeleteTableRequest().withDatabaseName(tableName.schema).withName(tableName.name)
      IO {
        glue.deleteTable(deleteRequest)
      }.void
    }

    "creating a partition" should "set format parameters" in {
      import scala.collection.JavaConverters._

      val scenario = for {
        _ <- initTable(partitionedTable)
        metastore = new GlueMetastore[IO](glue)
        dateCol = PartitionColumn("date")
        partition = Partition(dateCol, "2019-01-01")
        version <- Version.generateVersion
        _ <- metastore.addPartition(partitionedTable.name, partition, version)
        req = new GetPartitionsRequest()
          .withTableName(partitionedTable.name.name)
          .withDatabaseName(partitionedTable.name.schema)
      } yield (glue.getPartitions(req).getPartitions.asScala, version)

      val (partitions, version) = scenario.guarantee(deleteTable(partitionedTable.name)).unsafeRunSync()

      partitions should have size 1
      partitions.head.getStorageDescriptor.getLocation shouldBe s"${tableLocation}date=2019-01-01/${version.label}"
      partitions.head.getStorageDescriptor.getInputFormat shouldBe testInputFormat
      partitions.head.getStorageDescriptor.getOutputFormat shouldBe testOutPutFormat
      partitions.head.getStorageDescriptor.getSerdeInfo.getSerializationLibrary shouldBe testSerialisationLib
    }

    "updating a partition version" should "preserve format parameters" in {
      import scala.collection.JavaConverters._

      val scenario = for {
        _ <- initTable(partitionedTable)
        metastore = new GlueMetastore[IO](glue)
        dateCol = PartitionColumn("date")
        partition = Partition(dateCol, "2019-01-01")
        getPartitionsReq = new GetPartitionsRequest()
          .withTableName(partitionedTable.name.name)
          .withDatabaseName(partitionedTable.name.schema)
        version1 <- Version.generateVersion
        version2 <- Version.generateVersion
        _ <- metastore.addPartition(partitionedTable.name, partition, version1)
        partitionsBeforeUpdate = glue.getPartitions(getPartitionsReq).getPartitions.asScala
        _ <- metastore.updatePartitionVersion(partitionedTable.name, partition, version2)
        partitionsAfterUpdate = glue.getPartitions(getPartitionsReq).getPartitions.asScala

      } yield (partitionsBeforeUpdate, partitionsAfterUpdate, version1, version2)

      val (partitionsBeforeUpdate, partitionsAfterUpdate, expectedVersionBeforeUpdate, expectedVersionAfterUpdate) =
        scenario.guarantee(deleteTable(partitionedTable.name)).unsafeRunSync()

      partitionsBeforeUpdate should have size 1
      partitionsBeforeUpdate.head.getStorageDescriptor.getLocation shouldBe s"${tableLocation}date=2019-01-01/${expectedVersionBeforeUpdate.label}"
      shouldContainFormatParams(partitionsBeforeUpdate.head.getStorageDescriptor)

      partitionsAfterUpdate should have size 1
      partitionsAfterUpdate.head.getStorageDescriptor.getLocation shouldBe s"${tableLocation}date=2019-01-01/${expectedVersionAfterUpdate.label}"
      shouldContainFormatParams(partitionsAfterUpdate.head.getStorageDescriptor)
    }

    "updating a table location" should "preserve format parameters" in {

      val scenario = for {
        _ <- initTable(snapshotTable)
        metastore = new GlueMetastore[IO](glue)
        version <- Version.generateVersion
        _ <- metastore.updateTableLocation(snapshotTable.name, version)
        getTableReq = new GetTableRequest()
          .withName(snapshotTable.name.name)
          .withDatabaseName(snapshotTable.name.schema)
        glueTable <- IO { glue.getTable(getTableReq) }

      } yield (glueTable.getTable, version)

      val (updatedTable, expectedVersion) =
        scenario.guarantee(deleteTable(snapshotTable.name)).unsafeRunSync()

      updatedTable.getStorageDescriptor.getLocation shouldBe s"$tableLocation${expectedVersion.label}"
      shouldContainFormatParams(updatedTable.getStorageDescriptor)
    }

    def shouldContainFormatParams(storageDescriptor: StorageDescriptor): Assertion = {
      storageDescriptor.getInputFormat shouldBe testInputFormat
      storageDescriptor.getOutputFormat shouldBe testOutPutFormat
      storageDescriptor.getSerdeInfo.getSerializationLibrary shouldBe testSerialisationLib
    }
  }

}
