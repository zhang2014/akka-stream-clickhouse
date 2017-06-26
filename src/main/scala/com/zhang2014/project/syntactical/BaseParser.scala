package com.zhang2014.project.syntactical

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

trait BaseParser extends StandardTokenParsers
{
  protected lazy val identifier = {
    ident ^^ {
      case s if !s.endsWith("`") => s
      case s if !s.startsWith("`") => s
      case s => s.substring(1, s.length - 1)
    }
  }

  protected lazy val dataType: Parser[String] = {
    dataTypeWithBrackets | "Date" | "String" | "Int8" | "Int16" | "Int32" | "Int64" |
      "UInt8" | "UInt16" | "UInt32" | "UInt64" | "Float32" | "Float64" | tupleDataType
  }

  protected lazy val tableNameWithDB: Parser[(String, String)] = ((identifier ~ ".") ?) ~ identifier ^^ {
    case None ~ table => "default" -> table
    case Some(database ~ _) ~ table => database -> table
  }

  private lazy val tupleDataType = "Tuple" ~ "(" ~ dataType ~ (("," ~ dataType) *) ~ ")" ^^
    { case _ ~ _ ~ head ~ tail ~ _ => (head :: tail).mkString("Tuple(", ",", ")") }

  private lazy val dataTypeWithBrackets = "(" ~ dataType ~ ")" ^^ { case _ ~ t ~ _ => t }

  lexical.reserved +=
    ("Tuple", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Date", "String")

  lexical.reserved ++= Token.values().map(_.name())
  lexical.delimiters ++= Delimiters.values().map(_.getValue)

  implicit def token2StringParser(token: Token): Parser[String] = token.name()

  implicit def delimiter2StringParser(delimiters: Delimiters): Parser[String] = delimiters.getValue
}
