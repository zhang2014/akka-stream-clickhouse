package com.zhang2014.project.source

import java.io._
import java.util.{Date, NoSuchElementException}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.google.common.io.LittleEndianDataInputStream
import com.zhang2014.project.misc._

object ColumnSource
{
  def apply[T](dataType: String, file: String, range: Range = CompressedRange(0, -1, 0, -1))
    (implicit system: ActorSystem, materializer: Materializer): Source[T, NotUsed] = Source
    .fromGraph(new ColumnSource[T](dataType, new ByteBufferSourceInputStream(FileSource(file, range))))

  private final class ColumnSource[T](dataType: String, bin: InputStream) extends GraphStage[SourceShape[T]]
  {
    lazy val out   = shape.out
    lazy val input = new LittleEndianDataInputStream(bin)
    lazy val shape = SourceShape(Outlet[T]("ClickHouse-Column-Source"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            tryReadNext[T](dataType) match {
              case None => completeStage()
              case Some(nextValue) => push(out, nextValue)
            }
          }
        }
      )
    }

    private def tryReadNext[A](tpe: String): Option[A] = {
      import scala.concurrent.duration._
      try {
        val nextValue = tpe match {
          case "Date" => tryReadNext[Int]("UInt16").map(_.days.toMillis).map(new Date(_))
          case "Int8" => Some(input.readByte())
          case "Int32" => Some(input.readInt())
          case "Int64" => Some(input.readLong())
          case "Int16" => Some(input.readShort())
          case "Float32" => Some(input.readFloat())
          case "Float64" => Some(input.readDouble())
          case "VarInt" => Some(ReadUtility.readVarInt(input))
          case "UInt16" => tryReadNext[Short]("Int16").map(ReadUtility.int2UInt)
          case "UInt64" => tryReadNext[Long]("Int64").map(ReadUtility.int2UInt)
          case "String" => tryReadNext[Long]("VarInt").map(length => ReadUtility.readUTF(length.toInt, input))
          case _ if tpe.startsWith("Tuple(") =>
            tpe.substring(6, tpe.length - 1).split(",").foldLeft(Option(List.empty[Any])) {
              case (reducer, elements) => reducer.flatMap(l => tryReadNext[Any](elements).map(x => l :+ x))
            }.map(ReadUtility.list2Product).map(Some.apply).getOrElse(Option.empty[A])
        }
        nextValue.map(_.asInstanceOf[A])
      }
      catch {
        case _: EOFException => Option.empty[A]
        case _: NoSuchElementException => Option.empty[A]
        case ex => println(ex); throw ex
      }
    }
  }

}
