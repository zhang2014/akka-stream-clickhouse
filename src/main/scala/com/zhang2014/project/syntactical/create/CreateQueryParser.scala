package com.zhang2014.project.syntactical.create

import com.zhang2014.project.syntactical.ast.{CreateAST, ColumnAST}
import com.zhang2014.project.syntactical.BaseParser

trait CreateQueryParser extends BaseParser with EngineQueryParser
{
  lazy val createQuery = "ATTACH" ~ "TABLE" ~ identifier ~ "(" ~ columnsPtr ~ ")" ~ enginePtr ^^
    { case _ ~ _ ~ tableName ~ _ ~ columns ~ _ ~ engine => CreateAST(tableName, columns, engine) }

  private lazy val columnPtr = identifier ~ dataType ^^ { case name ~ tpe => ColumnAST(name, tpe) }

  private lazy val columnsPtr = columnPtr ~ (("," ~ columnPtr) *) ^^ { case head ~ tail => head :: tail.map(_._2) }

  lexical.reserved +=("ATTACH", "TABLE")
  lexical.delimiters +=("(", ")", ",", "=")
}
