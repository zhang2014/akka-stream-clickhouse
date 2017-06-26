package com.zhang2014.project.core

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.{Matchers, WordSpec}

class LookupIndicesInterpreterTest extends WordSpec with Matchers
{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()
  "LookupIndicesInterpreter" should {

    "read" in {
      val dir = new File(getClass.getClassLoader.getResource("resource@1/").toURI)
      LookupIndicesInterpreter(dir.getAbsolutePath, "default", "test_table_1", 197001).execute()
    }

  }
}
