package com.zhang2014.project.source

import akka.NotUsed
import akka.actor.ActorSystem
import akka.ext.Implicit._
import akka.stream._
import akka.stream.scaladsl.Source
import com.zhang2014.project.misc.{CompressedRange, Range, UnCompressedRange}

object DataPartSource
{
  def apply(dir: String, columns: List[String] = Nil, indices: List[String] = Nil)
    (implicit system: ActorSystem, materializer: Materializer): Source[List[(String, String, Any)], NotUsed] =
  {
    val info = loadAllColumnsInfo(dir)
    createRangeSource(dir, indices.map(name => name -> info(name)).toMap, columns).flatMapConcat { columnsRange =>
      columnsRange.zipWithIndex.map { case (range, idx) =>
        val columnName = columns(idx)
        val columnDataType = info(columnName)
        ColumnSource[Any](columnDataType, s"$dir/$columnName.bin", range).map(v => (columnName, columnDataType, v))
          .map(_ :: Nil)
      }.reduce((l, r) => l.zipWith(r)(_ ++ _))
    }
  }

  private def loadAllColumnsInfo(dir: String) = {
    val firstDesc = "columns format version: 1"
    val `firstDesc` :: secondDesc :: columnsDesc = scala.io.Source.fromFile(s"$dir/columns.txt").getLines().toList
    val Array(columnCountDesc, "columns:") = secondDesc.split(" ", 2)

    require(columnCountDesc.toInt == columnsDesc.length)
    columnsDesc.map(_.split(" ", 2)).map { case Array(name, tpe) => name.substring(1, name.length - 1) -> tpe }.toMap
  }

  private def createRangeSource(dir: String, indices: Map[String, String], columns: List[String])
    (implicit system: ActorSystem, materializer: Materializer) =
  {
    indices.toList match {
      case Nil => Source.single[List[Range]](columns.map(_ => CompressedRange(0, -1, 0, -1)))
      case _ => createPrimaryKeySource(dir, indices).zip(createMarkKeySource(dir, columns)).filter(x => true).map(_._2)
    }
  }

  private def createPrimaryKeySource(dir: String, indices: Map[String, String])
    (implicit system: ActorSystem, materializer: Materializer) =
  {
    val keys = indices.keys.toArray
    ColumnSource[Product](indices.values.mkString("Tuple(", ",", ")"), s"$dir/primary.idx", UnCompressedRange(0, -1))
      .map(key => key.productIterator.zipWithIndex.map(x => keys(x._2) -> x._1).toMap)
  }

  private def createMarkKeySource(dir: String, columns: List[String])
    (implicit system: ActorSystem, materializer: Materializer): Source[List[Range], NotUsed] =
  {
    columns.map(name => ColumnSource[(Long, Long)]("Tuple(Int64,Int64)", s"$dir/$name.mrk", UnCompressedRange(0, -1)))
      .map(_.mapDependsWindow(2)(seq => seq.head -> seq.getOrElse(1, -1L -> -1L)))
      .map(f => f.map { case (begin, end) => CompressedRange(begin._1, end._1, begin._2, end._2) })
      .map(_.map(_ :: Nil)).reduce((l, r) => l.zipWith(r)((l1, r1) => l1 ++ r1))
  }
}
