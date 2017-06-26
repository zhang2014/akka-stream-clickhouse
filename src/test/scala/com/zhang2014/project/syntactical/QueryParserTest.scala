package com.zhang2014.project.syntactical

import com.zhang2014.project.syntactical.QueryParser._
import com.zhang2014.project.syntactical.ast._
import org.scalatest.{Matchers, WordSpec}

class QueryParserTest extends WordSpec with Matchers
{
  "QueryParser" should {

    "successfully parse create table query" in {
      val createSQL = "ATTACH TABLE test_table1 ( date Date, id Int32, name String ) ENGINE = MergeTree(date, name, 8192)"
      QueryParser.phrase(QueryParser.query)(createSQL).get should ===(
        CreateAST(
          "test_table1",
          ColumnAST("date", "Date") :: ColumnAST("id", "Int32") :: ColumnAST("name", "String") :: Nil,
          EngineAST("MergeTree", "date", "name" :: Nil, 8192)
        )
      )
    }

    "select 1" in {
      QueryParser.phrase(QueryParser.query)("SELECT 1,1+1").get should ===(
        SelectAST(
          distinct = false,
          NumberLiteralExpression(1) ::
            FunctionExpression("plus", NumberLiteralExpression(1) :: NumberLiteralExpression(1) :: Nil) :: Nil,
          None, None, None
        )
      )
    }

    "select system.number" in {
      QueryParser.phrase(QueryParser.query)("SELECT * FROM system.numbers LIMIT 10").get should ===(
        SelectAST(
          distinct = false,
          IdentifierExpression("*") :: Nil,
          Some(SingleTable(IdentifierExpression("system.numbers"), isFinal = false)),
          limit = Some(LimitAST(0, 10))
        )
      )
    }
    //TODO:覆盖ClickHouse的SQL测试文件 @see:https://github.com/yandex/ClickHouse/tree/master/dbms/tests/queries/0_stateless
  }

  private implicit def string2Scan(string: String): lexical.Scanner = new lexical.Scanner(string)
}
