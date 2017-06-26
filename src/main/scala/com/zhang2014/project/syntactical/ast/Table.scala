package com.zhang2014.project.syntactical.ast

trait Table extends AST

case class ArrayJoinTable(kind: String, table: Table, array: List[Expression]) extends Table

case class
JoinTable(kind: String, locality: String, strictness: String, left: Table, right: Table, condition: List[Expression])
  extends Table

case class SingleTable(table: Expression, isFinal: Boolean) extends Table
