package com.zhang2014.project.syntactical.select

import com.zhang2014.project.syntactical.BaseParser
import com.zhang2014.project.syntactical.ast._

trait ExpressionParser extends BaseParser
{

  import com.zhang2014.project.syntactical.Token._
  import com.zhang2014.project.syntactical.Delimiters._

  lazy val expression: Parser[Expression] = lambdaExpression

  lazy val expressionList = expression ~ ((COMMA ~ aliasExpression(expression)) *) ^^
    { case head ~ tail => head :: tail.map(_._2) }

  lazy val expressionListWithAlias = aliasExpression(expression) ~ ((COMMA ~ aliasExpression(expression)) *) ^^
    { case head ~ tail => head :: tail.map(_._2) }

  protected def aliasExpression(aliasExpr: Parser[Expression]) = aliasExpr ~ ((AS ~ identifier) ?) ^^
    { case expr ~ alias => alias.map(x => AliasExpression(expr, x._2)).getOrElse(expr) }


  protected lazy val lambdaExpression = {
    val arguments = identifier ~ ((COMMA ~ identifier) *) ^^ { case head ~ tail => head :: tail.map(_._2) }
    val lambdaExpression = LEFT_BRACKET ~ arguments ~ RIGHT_BRACKET ~ "->" ~ ternaryOperatorExpression ^^ {
      case _ ~ param ~ _ ~ _ ~ body =>
        FunctionExpression("lambda", FunctionExpression("tuple", param.map(IdentifierExpression)) :: body :: Nil)
    }
    lambdaExpression | ternaryOperatorExpression
  }


  protected lazy val logicalOrExpression = {
    logicalAndExpression ~ ((OR ~ logicalAndExpression) ?) ^^ {
      case head ~ tail => tail.map(x => FunctionExpression("or", head :: x._2 :: Nil)).getOrElse(head)
    }
  }

  protected lazy val logicalAndExpression = {
    logicalNotExpression ~ ((AND ~ logicalNotExpression) ?) ^^ {
      case head ~ tail => tail.map(x => FunctionExpression("and", head :: x._2 :: Nil)).getOrElse(head)
    }
  }


  protected lazy val logicalNotExpression = {
    (NOT ?) ~ nullityCheckingExpression ^^
      { case head ~ tail => head.map(x => FunctionExpression("not", tail :: Nil)).getOrElse(tail) }
  }

  protected lazy val ternaryOperatorExpression = {
    val body = QUESTION ~ logicalOrExpression ~ COLON ~ logicalOrExpression ^^
      { case _ ~ body1 ~ _ ~ body2 => (body1, body2) }

    logicalOrExpression ~ (body ?) ^^
      { case c ~ b => b.map(x => FunctionExpression("if", c :: x._1 :: x._2 :: Nil)).getOrElse(c) }
  }

  protected lazy val nullityCheckingExpression = {
    val checkNull = IS ~ (NOT ?) ~ NULL ^^ { case _ ~ not ~ _ => not.map(_ => "isNotNull").getOrElse("isNull") }
    comparisonExpression ~ (checkNull ?) ^^
      { case expr ~ fn => fn.map(name => FunctionExpression(name, expr :: Nil)).getOrElse(expr) }
  }

  protected lazy val comparisonExpression = {
    val operators = Map[String, String](
      "NOT IN" -> "notIn", "GLOBAL IN" -> "globalIn", "GLOBAL NOT IN" -> "globalNotIn",
      "<" -> "less", ">" -> "greater", "=" -> "equals", "LIKE" -> "like", "NOT LIKE" -> "notLike", "IN" -> "in",
      "==" -> "equals", "!=" -> "notEquals", "<>" -> "notEquals", "<=" -> "lessOrEquals", ">=" -> "greaterOrEquals"
    )
    val second = operators.keys.map(keyword).reduce(_ | _) ~ betweenExpression ^^
      { case fn ~ s => operators.get(fn).get -> s }
    betweenExpression ~ (second ?) ^^
      { case head ~ tail => tail.map(x => FunctionExpression(x._1, head :: x._2 :: Nil)).getOrElse(head) }
  }

  protected lazy val betweenExpression = {
    val between = BETWEEN ~ concatExpression ~ AND ~ concatExpression ^^ { case _ ~ min ~ _ ~ max => min -> max }
    concatExpression ~ (between ?) ^^ {
      case head ~ bw => bw.map { x =>
        FunctionExpression(
          "and",
          FunctionExpression("greaterOrEquals", head :: x._1 :: Nil) ::
            FunctionExpression("lessOrEquals", head :: x._2 :: Nil) ::
            Nil
        )
      }.getOrElse(head)
    }
  }

  protected lazy val concatExpression = {
    additiveExpression ~ (("||" ~ additiveExpression) ?) ^^
      { case head ~ tail => tail.map(x => FunctionExpression("concat", head :: x._2 :: Nil)).getOrElse(head) }
  }

  protected lazy val additiveExpression = {
    val additive = (PLUS | MINUS) ~ multiplicativeExpression ^^
      { case "+" ~ expr => "plus" -> expr case "-" ~ expr => "minus" -> expr }

    multiplicativeExpression ~ (additive ?) ^^
      { case first ~ seconds => seconds.map(x => FunctionExpression(x._1, first :: x._2 :: Nil)).getOrElse(first) }
  }

  protected lazy val multiplicativeExpression = {
    val multiplicative = ((MULTIPLY | DIVIDE | MODULO) ~ unaryMinusExpression) ^^
      { case "*" ~ expr => "multiply" -> expr case "/" ~ expr => "divide" -> expr case "%" ~ expr => "modulo" -> expr }

    unaryMinusExpression ~ (multiplicative ?) ^^
      { case first ~ seconds => seconds.map(x => FunctionExpression(x._1, first :: x._2 :: Nil)).getOrElse(first) }
  }

  val NEGATE: String = "-"
  protected lazy val unaryMinusExpression = {
    (NEGATE ~ literalExpression) ^^
      { case _ ~ NumberLiteralExpression(v) => NumberLiteralExpression(0 - v.doubleValue()) } |
      ((NEGATE ?) ~ tupleElementExpression) ^^
        { case h ~ t => h.map(_ => FunctionExpression("negate", t :: Nil)).getOrElse(t) }
  }


  protected lazy val tupleElementExpression = {
    arrayElementExpression ~ ((DOT ~ unsignedInteger) ?) ^^
      { case head ~ tail => tail.map(x => FunctionExpression("tupleElement", head :: x._2 :: Nil)).getOrElse(head) }
  }


  protected lazy val arrayElementExpression = {
    val arrayIdx = ARRAY_LEFT ~ expression ~ ARRAY_RIGHT ^^ { case _ ~ idx ~ _ => idx }
    expressionElement ~ (arrayIdx ?) ^^
      { case head ~ tail => tail.map(x => FunctionExpression("arrayElement", head :: x :: Nil)).getOrElse(head) }
  }

  protected lazy val unsignedInteger = numericLit ^^ { case v => NumberLiteralExpression(v.toInt) }

  protected lazy val expressionElement = {
    subQueryExpression | parenthesisExpression | arrayOfLiteralsExpression | arrayExpression | literalExpression |
      castExpression | qualifiedAsteriskExpression | asteriskExpression | compoundIdentifierExpression
  }

  protected lazy val subQueryExpression = LEFT_BRACKET ~ selectQuery ~ RIGHT_BRACKET ^^
    { case _ ~ query ~ _ => SubSelectExpression(query) }

  protected lazy val parenthesisExpression = {
    LEFT_BRACKET ~ expressionList ~ RIGHT_BRACKET ^^ {
      case _ ~ elements ~ _ if elements.length == 1 => elements.head
      case _ ~ elements ~ _ => FunctionExpression("tuple", elements)
    }
  }

  protected lazy val arrayExpression = ARRAY_LEFT ~ expressionListWithAlias ~ ARRAY_RIGHT ^^
    { case _ ~ elements ~ _ => FunctionExpression("array", elements) }

  protected lazy val arrayOfLiteralsExpression = {
    ARRAY_LEFT ~ literalExpression ~ ((COMMA ~ literalExpression) *) ~ ARRAY_RIGHT ^^
      { case _ ~ head ~ tail ~ _ => ArrayLiteralExpression(head :: tail.map(_._2)) }
  }

  protected lazy val castExpression = {
    caseExpression | (
      CAST ~ LEFT_BRACKET ~ expression ~ COMMA ~ stringLit ~ RIGHT_BRACKET ^^
        { case _ ~ _ ~ expr ~ _ ~ tpe ~ _ => expr :: StringLiteralExpression(tpe) :: Nil } |
        CAST ~ LEFT_BRACKET ~ aliasExpression(expression) ~ RIGHT_BRACKET ^^
          { case _ ~ _ ~ (x: AliasExpression) ~ _ => x.expressionAST :: StringLiteralExpression(x.alias) :: Nil }) ^^
      { case params => FunctionExpression("cast", params) }
  }

  protected lazy val compoundIdentifierExpression = identifier ~ ((DOT ~ identifier) *) ^^
    { case head ~ tail => IdentifierExpression((head :: tail.map(_._2)).mkString(".")) }

  protected lazy val asteriskExpression = ASTERISK ^^ { case _ => IdentifierExpression("*") }

  protected lazy val qualifiedAsteriskExpression = compoundIdentifierExpression ~ DOT ~ ASTERISK ^^
    { case (x: IdentifierExpression) ~ _ ~ _ => x.copy(x + ".*") }

  protected lazy val caseExpression: Parser[Expression] = {
    val whenExpr = WHEN ~ expression ^^ { case _ ~ expr => expr }
    val thenExpr = THEN ~ expression ^^ { case _ ~ expr => expr }

    CASE ~ (expression ?) ~ ((whenExpr ~ thenExpr) +) ~ ELSE ~ expression ~ END ^^ {
      case _ ~ None ~ whenThenExpr ~ _ ~ elseExpr ~ _ =>
        FunctionExpression("caseWithoutExpr", whenThenExpr.flatMap(x => x._1 :: x._2 :: Nil) :+ elseExpr)
      case _ ~ Some(caseExpr) ~ whenThenExpr ~ _ ~ elseExpr ~ _ =>
        FunctionExpression("caseWithExpr", (caseExpr :: whenThenExpr.flatMap(x => x._1 :: x._2 :: Nil)) :+ elseExpr)
    } | functionExpression
  }

  protected lazy val functionExpression = {
    identifier ~ LEFT_BRACKET ~ (DISTINCT ?) ~ expressionList ~ RIGHT_BRACKET ^^ {
      case fn ~ _ ~ None ~ expressions ~ _ => FunctionExpression(fn, expressions)
      case fn ~ _ ~ Some(_) ~ expressions ~ _ => FunctionExpression(s"${fn}Distinct", expressions)
    } | identifier ~ LEFT_BRACKET ~ expressionList ~ RIGHT_BRACKET ~ LEFT_BRACKET ~ (DISTINCT ?) ~ expressionList ~
      RIGHT_BRACKET ^^ {
      case fn ~ _ ~ parameters ~ _ ~ _ ~ None ~ expressions ~ _ => FunctionExpression(fn, expressions, parameters)
      case fn ~ _ ~ parameters ~ _ ~ _ ~ Some(_) ~ expressions ~ _ =>
        FunctionExpression(s"${fn}Distinct", expressions, parameters)
    }
  }

  protected lazy val literalExpression = {
    NULL ^^ { case _ => NullLiteralExpression } |
      numericLit ^^ { case x => NumberLiteralExpression(x.toInt) } |
      floatNumericLit ^^ { case x => NumberLiteralExpression(x.toDouble) } |
      stringLit ^^ { case x => StringLiteralExpression(x) }
    //TODO: '' string
  }

  protected lazy val floatNumericLit = numericLit ~ DOT ~ numericLit ^^ { case h ~ _ ~ t => s"$h.$t" }

  protected def selectQuery: Parser[SelectAST]

}
