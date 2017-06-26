package com.zhang2014.project.syntactical.select

import com.zhang2014.project.syntactical.ast._
import com.zhang2014.project.syntactical.{BaseParser, Delimiters, Token}

trait SelectQueryParser extends BaseParser with ExpressionParser
{

  import Token._
  import Delimiters._

  //TODO:SAMPLE query
  lazy val selectQuery: Parser[SelectAST] = {
    SELECT ~ (DISTINCT ?) ~ expressionListWithAlias ~ (fromQuery ?) ~ (preWhereExpression ?) ~
      (whereExpression ?) ~ (groupByExpression ?) ~ ((WITH ~ TOTALS) ?) ~ (havingExpression ?) ~ (orderByExpression ?) ~
      (limitByExpression ?) ~ (limitExpression ?) ~ (settingExpression ?) ~ (unionExpression ?) ^^ {
      case _ ~ distinct ~ columns ~ table ~ preWhere ~ where ~
        groupBy ~ total ~ having ~ orderBy ~ limitBy ~ limit ~ setting ~ union =>
        SelectAST(
          distinct.isDefined, columns, table, preWhere, where, groupBy.getOrElse(Nil),
          total.isDefined, having, orderBy.getOrElse(Nil), limit, limitBy, setting.getOrElse(Nil), union
        )
    }
  }


  private lazy val fromQuery = FROM ~ tablesExpression ^^ { case _ ~ tables => tables }

  private lazy val tablesExpression = {
    val on = ON ~ logicalOrExpression ^^ { case _ ~ expr => expr :: Nil }
    val using = USING ~ LEFT_BRACKET ~ expressionList ~ RIGHT_BRACKET ^^ { case _ ~ _ ~ expr ~ _ => expr }

    val arrayJoinQuery = ((LEFT | INNER) ?) ~ ARRAY ~ JOIN ~ expressionList ^^
      { case tpe ~ _ ~ _ ~ arrayList => tpe.getOrElse(INNER) -> arrayList }

    val tailTableExpression = ("," ~ tableElementExpression) ^^
      { case _ ~ table => (COMMA_STR, UNSPECIFIED, UNSPECIFIED, table, List.empty[Expression]) } |
      ((GLOBAL | LOCAL) ?) ~ ((ANY | ALL) ?) ~ (INNER | LEFT | RIGHT | FULL | CROSS) ~
        (OUTER ?) ~ JOIN ~ tableElementExpression ~ (using | on) ^^ {
        case locality ~ strictness ~ tpe ~ _ ~ _ ~ table ~ expr =>
          (tpe, locality.getOrElse(UNSPECIFIED), strictness.getOrElse(UNSPECIFIED), table, expr)
      }

    tableElementExpression ~ ((arrayJoinQuery | tailTableExpression) *) ^^ {
      case table ~ tail => tail.foldLeft(table: Table) {
        case (l: Table, (tpe: String, list: List[Expression])) => ArrayJoinTable(tpe, l, list)
        case (l: Table, (tpe: String, locality: String, strictness: String, r: Table, expr: List[Expression])) =>
          JoinTable(tpe, locality, strictness, l, r, expr)
      }
    }
  }

  private lazy val tableElementExpression = {
    (subQueryExpression | functionExpression | compoundIdentifierExpression) ~ (FINAL ?) ^^
      { case table ~ isFinal => SingleTable(table, isFinal.isDefined) }
  }

  private lazy val unionExpression = UNION ~ ALL ~ selectQuery ^^ { case _ ~ _ ~ query => query }

  private lazy val whereExpression  = WHERE ~ expression ^^ { case _ ~ expr => expr }
  private lazy val havingExpression = HAVING ~ expression ^^ { case _ ~ expr => expr }

  private lazy val preWhereExpression = PREWHERE ~ expression ^^ { case _ ~ expr => expr }
  private lazy val groupByExpression  = GROUP ~ BY ~ expressionList ^^ { case _ ~ _ ~ exprs => exprs }

  private lazy val limitByExpression = LIMIT ~ numericLit ~ BY ~ expressionList ^^
    { case _ ~ length ~ _ ~ columns => LimitByAST(length.toInt, columns) }

  private lazy val limitExpression = LIMIT ~ numericLit ~ ((COMMA ~ numericLit) ?) ^^ { case _ ~ offset ~ length =>
    LimitAST(length.map(_ => offset.toLong).getOrElse(0), length.map(_._2.toLong).getOrElse(offset.toLong))
  }

  private lazy val orderByExpression = {
    val collateExpr = (COLLATE ~ stringLit) ^^ { case _ ~ str => str }
    val nullOrderMode = NULL ~ (FIRST | LAST) ^^ { case _ ~ order => "FIRST" == order }
    val columnOrderMode = (ASCENDING | DESCENDING | ASC | DESC) ^^ { case order => order.startsWith("ASC") }

    val orderByElement = expression ~ (columnOrderMode ?) ~ (nullOrderMode ?) ~ (collateExpr ?) ^^ {
      case expr ~ order ~ nullOrder ~ collate =>
        OrderByAST(expr, order.getOrElse(true), nullOrder.getOrElse(order.getOrElse(true)), collate)
    }

    ORDER ~ BY ~ orderByElement ~ ((COMMA ~ orderByElement) *) ^^ {
      case _ ~ _ ~ head ~ tail => head :: tail.map(_._2)
    }
  }

  private lazy val settingExpression = {
    val nameAndValuePart = identifier ~ EQUALS ~ literalExpression ^^ { case name ~ _ ~ value => (name, value) }
    SETTINGS ~ nameAndValuePart ~ ((COMMA ~ nameAndValuePart) *) ^^ { case _ ~ head ~ tail => head :: tail.map(_._2) }
  }
}
