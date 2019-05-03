package com.gu.tableversions.spark

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.gu.tableversions.core.Partition.PartitionColumn
import org.scalatest.{FlatSpec, Matchers}

class KryoSpec extends FlatSpec with Matchers {

  /**
    * Not a very conclusive test.
    *
    * This test is trying to prove that PartitionColumn is kryo-serialisable.
    * There is an issue when the class in used within ophan-data-lake such that kryo fails with:
    *
    * java.lang.ClassCastException: com.gu.tableversions.core.Partition$PartitionColumn cannot be cast to java.lang.String
    *
    * The problem is (or seems to be) with the resulting class signature
    *
    * final case class PartitionColumn(val name : scala.Predef.String) extends scala.AnyVal with scala.Product with scala.Serializable
    *
    * => scala.AnyVal is the problem
    *
    * Sadly, this test below passes regardless from within table-versions
    * Once the first ophan-data-lake's versioned job happen, this test will be (could be) moved here.
    * Or alternatively, refrain from using AnyVal for spark-bound case classes.
    *
    */
  "kryo" should "not throw an exception upon serialising PartitionColumn" in {
    val kryo = new Kryo

    kryo.writeObject(new Output(1024, -1), PartitionColumn("col"))
  }
}