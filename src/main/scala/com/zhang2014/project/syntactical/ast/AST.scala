package com.zhang2014.project.syntactical.ast

trait AST

case class ColumnAST(name: String, dataType: String) extends AST

case class LookupIndices(dataBase: String, tableName: String, partition: Int) extends AST

case class CreateAST(table: String, columns: List[ColumnAST], engine: EngineAST) extends AST

case class EngineAST(name: String, primary: String, indices: List[String], blockSize: Int) extends AST

case class SelectAST(
  distinct: Boolean,
  columns: List[Expression],
  tables: Option[Table] = None,
  preWhere: Option[Expression] = None,
  where: Option[Expression] = None,
  groupBy: List[Expression] = Nil,
  withTotal: Boolean = false,
  having: Option[Expression] = None,
  orderBy: List[OrderByAST] = Nil,
  limit: Option[LimitAST] = None,
  limitBy: Option[LimitByAST] = None,
  settings: List[(String, Expression)] = Nil,
  unionQuery: Option[SelectAST] = None
) extends AST

case class LimitAST(offset: Long, limit: Long) extends AST

case class LimitByAST(length: Int, expressions: List[Expression]) extends AST

case class OrderByAST(expression: Expression, acs: Boolean, nullFirst: Boolean, collate: Option[String]) extends AST
