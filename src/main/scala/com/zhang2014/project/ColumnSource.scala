package com.zhang2014.project

import java.io.{InputStream, FileInputStream, RandomAccessFile}
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.google.common.io.LittleEndianDataInputStream
import com.zhang2014.project.misc._
object ColumnSource
{
  def apply[T](dataType: String, file: String, compressed: Boolean = true)
    (implicit system: ActorSystem, materializer: Materializer): Source[T, NotUsed] =
  {
    if (compressed) {
      Source.fromGraph(
        new ColumnSource[T](
          dataType, ByteBufferSourceInputStream(CompressedSource(new RandomAccessFile(file, "r").getChannel))
        )
      )
    } else {
      Source.fromGraph(new ColumnSource[T](dataType, new FileInputStream(file)))
    }
  }

  private final class ColumnSource[T](dataType: String, bin: InputStream)
    (implicit system: ActorSystem, materializer: Materializer) extends GraphStage[SourceShape[T]]
  {
    lazy val out            = shape.out
    lazy val shape          = SourceShape(Outlet[T]("ClickHouse-Column-Source"))
    lazy val streamWithType = newTypeInputStream(dataType, new LittleEndianDataInputStream(bin))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            streamWithType.flatMap(_.next) match {
              case None => completeStage()
              case Some(ele) => push(out, ele)
            }
          }
        }
      )
    }

    private def newTypeInputStream(tpe: String, input: LittleEndianDataInputStream): Option[TypeInputStream[T]] =
      tpe match {
        case "Date" => Some(DateInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "Int8" => Some(ByteInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "Int32" => Some(IntInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "Int64" => Some(LongInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "UInt64" => Some(UnsignedLongInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "Float" => Some(FloatInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "Int16" => Some(ShortInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case "String" => Some(StringInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
        case _ if tpe.startsWith("Tuple(") =>
          val subInputs = tpe.substring(6, tpe.length - 1).split(",").map(subType => newTypeInputStream(subType, input))
          Some(TupleInputStream(subInputs.map(_.get): _*)).map(_.asInstanceOf[TypeInputStream[T]])
    }
  }
}
