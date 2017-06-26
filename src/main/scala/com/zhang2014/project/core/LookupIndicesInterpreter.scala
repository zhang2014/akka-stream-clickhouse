package com.zhang2014.project.core

import java.io.{File, RandomAccessFile}

import akka.actor.ActorSystem
import akka.ext.Implicit._
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}
import com.zhang2014.project.misc.UnCompressedRange
import com.zhang2014.project.source.ColumnSource
import com.zhang2014.project.syntactical.QueryParser._
import com.zhang2014.project.syntactical.QueryParser
import com.zhang2014.project.syntactical.ast.CreateAST

case class LookupIndicesInterpreter(dir: String, database: String, tableName: String, partition: Int)
{
  def execute()(implicit system: ActorSystem, materializer: Materializer): Unit = {
    implicit val ec = system.dispatcher
    // PartName -> (IndexColumnName)* -> LoadMinSize
    val createAST = loadCreateQuery(database, tableName)
    val columnAndDataType = createAST.columns.map(x => x.name -> x.dataType).toMap
    val indicesDataType = createAST.engine.indices.map(columnAndDataType).mkString("Tuple(", ",", ")")

    new File(s"$dir/data/$database/$tableName").listFiles().filter(_.getName.substring(0, 6).toInt == partition)
      .filter(_.isDirectory).map(_.getAbsolutePath)
      .map(createRanges(indicesDataType, createAST.columns.map(_.name))).reduce(_ union _)
      .toMat(Sink.foreach(println))(Keep.right).run().result()
  }

  private def createRanges(dataType: String, columns: List[String])
    (implicit system: ActorSystem, materializer: Materializer) =
  { file: String =>
    ColumnSource[Product](dataType, s"$file/primary.idx", UnCompressedRange(0, -1)) zip
      columns.map { name =>
        ColumnSource[(Long, Long)]("Tuple(Int64,Int64)", s"$file/$name.mrk", UnCompressedRange(0, -1))
          .mapDependsWindow(2)(
            seq => seq.getOrElse(1, new RandomAccessFile(s"$file/$name.bin", "r").getChannel.size() -> 0L)._1 -
              seq.head._1
          )
      }.map(_.map(_ :: Nil)).reduce((l, r) => l.zipWith(r)((l1, r1) => l1 ++ r1))
  }

  private def loadCreateQuery(database: String, tableName: String) = {
    val createTableQuery = scala.io.Source.fromFile(s"$dir/metadata/$database/$tableName.sql").getLines().mkString("\n")
    QueryParser.phrase(QueryParser.query)(new lexical.Scanner(createTableQuery)).get.asInstanceOf[CreateAST]
  }
}
