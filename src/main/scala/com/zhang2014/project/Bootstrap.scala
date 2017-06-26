package com.zhang2014.project

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.zhang2014.project.core.QueryInterpreter
import com.zhang2014.project.syntactical.QueryParser
import com.zhang2014.project.syntactical.QueryParser._
import com.zhang2014.project.syntactical.ast.AST

import scala.xml.XML

object Bootstrap
{
  val usage = """"""


  type OptionMap = Map[String, Any]

  private def requireOptions = Array[String]("config-file")

  private implicit val system       = ActorSystem()
  private implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    if (args.length == 0) println(usage)
    val optionMap = createOptionMap(args.toList)

    //TODO:xml中的include与macro
    optionMap.get("config-file").map(_.asInstanceOf[String]).map(XML.loadFile)
      .zip(optionMap.get("query").map(_.asInstanceOf[String]).map(parseSQL)).foreach {
      case (configFile, success: Success[AST]) => QueryInterpreter(success.get, configFile, null).execute()
      case (configFile, Failure(errorMessage, nextParserResult)) => //TODO:错误提示
    }
  }

  private def createOptionMap(args: List[String]): OptionMap = args match {
    case Nil => Map.empty[String, Any]
    case "-query" :: sql :: tail => createOptionMap(tail) + ("query" -> sql)
    case "-config-file" :: dir :: tail => createOptionMap(tail) + ("config-file" -> dir)
  }

  private def parseSQL(sql: String) = QueryParser.phrase(QueryParser.query)(new lexical.Scanner(sql))
}
