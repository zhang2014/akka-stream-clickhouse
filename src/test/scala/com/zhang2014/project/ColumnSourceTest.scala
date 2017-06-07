package com.zhang2014.project

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.WordSpec
import scala.concurrent.duration._

class ColumnSourceTest extends WordSpec
{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()


  "ColumnSource" should {
    //  test_table_1:

    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 1980-01-01 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘
    //  ┌──eventDate─┬─eventId─┬─eventName─┬─count─┐
    //  │ 0000-00-00 │       1 │ OnClick   │     3 │
    //  └────────────┴─────────┴───────────┴───────┘

    "successfully read part data" in {
      val partURI = getClass.getClassLoader.getResource("test_table_1/19700101_19700101_2_2_0").toURI
      val source = ColumnSource[Int](new File(partURI).getAbsolutePath, "count")
      val sub = source.toMat(TestSink.probe[Int])(Keep.right).run()

      sub.request(2)
      sub.expectNext(3 seconds, 3)
      sub.expectComplete()
    }
  }

}
