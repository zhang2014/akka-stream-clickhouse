package com.zhang2014.project.syntactical.create

import com.zhang2014.project.syntactical.BaseParser
import com.zhang2014.project.syntactical.ast.EngineAST

trait EngineQueryParser extends BaseParser
{

  lazy val enginePtr: Parser[EngineAST] = "ENGINE" ~ "=" ~ mergeTreePtr ^^ { case _ ~ _ ~ engine => engine }


  private lazy val mergeTreePtr = identifier ~ "(" ~ identifier ~ "," ~ indicesPtr ~ "," ~ numericLit ~ ")" ^^
    { case name ~ _ ~ primary ~ _ ~ indices ~ _ ~ blockSize ~ _ => EngineAST(name, primary, indices, blockSize.toInt) }

  private lazy val singleIndex = identifier ^^ { case index => index :: Nil }

  private lazy val multiIndices = "(" ~ identifier ~ (("," ~ identifier) *) ~ ")" ^^
    { case _ ~ head ~ tail ~ _ => head :: tail.map(_._2) }

  private lazy val indicesWithBrackets = "(" ~ indicesPtr ~ ")" ^^ { case _ ~ x ~ _ => x }

  private lazy val indicesPtr: Parser[List[String]] = indicesWithBrackets | singleIndex | multiIndices

  lexical.reserved += "ENGINE"
  lexical.delimiters +=("(", ")", ",", "=")
}
