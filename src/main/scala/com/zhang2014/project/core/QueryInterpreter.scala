package com.zhang2014.project.core

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.zhang2014.project.Bootstrap.OptionMap
import com.zhang2014.project.syntactical.ast.{LookupIndices, AST}

import scala.xml.Elem

case class QueryInterpreter(ast: AST, xml: Elem, options: OptionMap)
{
  private lazy val dir = (xml \ "path").text

  def execute()(implicit system: ActorSystem, materializer: Materializer): Unit = ast match {
    case LookupIndices(db, table, partition) => LookupIndicesInterpreter(dir, db, table, partition).execute()
    case _ => //TODO:提示错误
  }
}
