package com.zhang2014.project.source

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import com.zhang2014.project.misc.UnCompressedRange
import org.scalatest.WordSpec

class PrimaryKeySourceTest extends WordSpec
{
  private implicit val system       = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  "PrimaryKeySource" should {

    //  test_table_1:

    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 1980-01-01 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘
    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 0000-00-00 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘

    "successfully read primary key" in {
      val partURI = getClass.getClassLoader.getResource("test_table_1/19700101_19700101_2_2_0").toURI
      val source = ColumnSource[(BigInt, String)](
        "Tuple(UInt64,String)", new File(partURI).getAbsolutePath + "/primary.idx", UnCompressedRange(0, -1)
      )
      val sub = source.toMat(TestSink.probe[(BigInt, String)])(Keep.right).run()

      import scala.concurrent.duration._
      sub.request(2)
      sub.expectNext(1000.seconds, BigInt(1) -> "OnClick")
      sub.expectComplete()
    }
  }
}
