package com.gu.tableversions.glue

import com.amazonaws.services.glue.model.{SerDeInfo, StorageDescriptor}
import org.scalatest.{FlatSpec, Matchers}

class GlueMetastoreSpec extends FlatSpec with Matchers {
  val testInputFormat = "org.fake.InputFormat"
  val testOutPutFormat = "org.fake.OutputFormat"
  val testSerialisationLib = "org.fake.serde.FakeSerde"

  "extractFormatParams" should "return a storage description with the format params" in {
    val serdeInfo = new SerDeInfo()
      .withSerializationLibrary(testSerialisationLib)
      .withName("shouldBeDropped")

    val descriptorWithExtraParams = new StorageDescriptor()
      .withSerdeInfo(serdeInfo)
      .withInputFormat(testInputFormat)
      .withOutputFormat(testOutPutFormat)
      .withLocation("shouldBeDropped")

    val actual = GlueMetastore.extractFormatParams(descriptorWithExtraParams)

    actual.getSerdeInfo.getSerializationLibrary shouldBe serdeInfo.getSerializationLibrary
    actual.getSerdeInfo.getName should be(null)
    actual.getOutputFormat shouldBe testOutPutFormat
    actual.getInputFormat shouldBe testInputFormat
    actual.getLocation should be(null)
  }

  it should "ignore empty serdeInfo without failing" in {
    val descriptorWithoutSerdeInfo =
      new StorageDescriptor().withInputFormat(testInputFormat).withOutputFormat(testOutPutFormat)
    val actual = GlueMetastore.extractFormatParams(descriptorWithoutSerdeInfo)
    actual.getInputFormat should be(testInputFormat)
    actual.getOutputFormat should be(testOutPutFormat)
    actual.getSerdeInfo should be(null)
  }

  it should "ignore empty serdeInfo with null library without failing" in {
    val serdeWithNoLibrary = new SerDeInfo().withName("someName")
    val descriptorWithNullLibrary = new StorageDescriptor()
      .withInputFormat(testInputFormat)
      .withOutputFormat(testOutPutFormat)
      .withSerdeInfo(serdeWithNoLibrary)
    val actual = GlueMetastore.extractFormatParams(descriptorWithNullLibrary)
    actual.getInputFormat should be(testInputFormat)
    actual.getOutputFormat should be(testOutPutFormat)
    actual.getSerdeInfo.getSerializationLibrary should be(null)
  }

  it should "ignore empty input or output format without failing" in {
    val serdeWithNoLibrary = new SerDeInfo().withSerializationLibrary(testSerialisationLib)
    val descriptorWithNoFormats = new StorageDescriptor().withSerdeInfo(serdeWithNoLibrary)
    val actual = GlueMetastore.extractFormatParams(descriptorWithNoFormats)
    actual.getInputFormat should be(null)
    actual.getOutputFormat should be(null)
    actual.getSerdeInfo.getSerializationLibrary shouldBe testSerialisationLib
  }

}
