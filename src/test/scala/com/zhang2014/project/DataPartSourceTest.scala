package com.zhang2014.project

import java.io.File
import java.math.BigInteger
import java.util.Date

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import com.zhang2014.project.DataPartSource.Record
import org.scalatest.WordSpec
import scala.concurrent.duration._

class DataPartSourceTest extends WordSpec
{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()

  "DataPartSource" should {

    //  test_table_1:

    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 1980-01-01 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘
    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 0000-00-00 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘

    "successfully read part data" in {
      val partURI = getClass.getClassLoader.getResource("test_table_1/19700101_19700101_2_2_0").toURI
      val partSource = DataPartSource(new File(partURI).getAbsolutePath)
      val sub = partSource.toMat(TestSink.probe[Record])(Keep.right).run()

      import DataType._
      sub.request(2)
      sub.expectNext(
        3 seconds,
        new Record(
          (DATE, "eventDate", new Date(0)) ::
            (UINT64, "eventId", BigInt(new BigInteger("1"))) ::
            (STRING, "eventName", "OnClick") ::
            (INT32, "count", 3) ::
            Nil
        )
      )
      sub.expectComplete()
    }
  }
}
