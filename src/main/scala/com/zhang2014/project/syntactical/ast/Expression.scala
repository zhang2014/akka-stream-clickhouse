package com.zhang2014.project.syntactical.ast

trait Expression extends AST

case object NullLiteralExpression extends Expression

case class NumberLiteralExpression(number: Number) extends Expression

case class StringLiteralExpression(values: String) extends Expression

case class ArrayLiteralExpression(elements: List[Expression]) extends Expression


case class IdentifierExpression(str: String) extends Expression

case class SubSelectExpression(selectQuery: SelectAST) extends Expression

case class AliasExpression(expressionAST: Expression, alias: String) extends Expression

case class FunctionExpression(name: String, arguments: List[Expression], parameters: List[Expression] = Nil)
  extends Expression
