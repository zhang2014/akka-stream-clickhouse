package com.zhang2014.project

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.scalatest.WordSpec

class ColumnSourceTest extends WordSpec
{
  val testResource = "/Users/coswde/Project/clickhouse-checksum/src/test/resources/test_table_1"

  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  "ColumnSource" should {

    "successfully read part data" in {
      val source = ColumnSource[Long](testResource, "id")
      source.to(Sink.foreach(println)).run()
      //TODO:match
    }
  }

}
