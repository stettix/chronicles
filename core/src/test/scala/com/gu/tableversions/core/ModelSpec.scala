package com.gu.tableversions.core

import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

class ModelSpec extends FlatSpec with PropertyChecks {

  "A model type" should "do the right thing" in {
    assert(2 + 2 === 4)
  }

}
