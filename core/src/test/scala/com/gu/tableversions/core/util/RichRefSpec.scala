package com.gu.tableversions.core.util

import cats.effect.IO
import cats.effect.concurrent.Ref
import org.scalatest.{FlatSpec, Matchers}

class RichRefSpec extends FlatSpec with Matchers {

  import RichRef._

  // A test update function
  val doubleIfEven: Int => Either[Exception, Int] = n =>
    if (n % 2 == 0) Right(n * 2) else Left(new Exception(s"Won't double an odd number: $n"))

  "Applying a valid update" should "update the Ref and return success" in {
    val test = for {
      ref <- Ref.of[IO, Int](42)
      _ <- ref.modifyEither(doubleIfEven)
      finalValue <- ref.get
    } yield finalValue

    val result = test.unsafeRunSync()
    result shouldBe 42 * 2
  }

  "Applying an update that isn't valid" should "return the failure" in {
    val test = for {
      ref <- Ref.of[IO, Int](3)
      _ <- ref.modifyEither(doubleIfEven)
    } yield ()

    val ex = the[Exception] thrownBy test.unsafeRunSync()
    ex.getMessage should include("Won't double an odd number: 3")
  }

}
