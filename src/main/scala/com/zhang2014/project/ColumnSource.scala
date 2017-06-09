package com.zhang2014.project

import java.io.RandomAccessFile
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream._
import com.google.common.io.LittleEndianDataInputStream
import com.zhang2014.project.misc._

import scala.reflect.ClassTag

object ColumnSource
{
  def apply[T: ClassTag](file: String)
    (implicit system: ActorSystem, materializer: Materializer): Source[T, NotUsed] = Source
    .fromGraph(new ColumnSource[T](new RandomAccessFile(file, "r").getChannel))

  private final class ColumnSource[T](bin: FileChannel)
    (implicit m: ClassTag[T], system: ActorSystem, materializer: Materializer) extends GraphStage[SourceShape[T]]
  {
    lazy val out   = shape.out
    lazy val shape = SourceShape(Outlet[T]("ClickHouse-Column-Source"))

    lazy val stream         = ByteBufferSourceInputStream(CompressedSource(bin))(system, materializer)
    lazy val streamWithType = createTypeInputStream(new LittleEndianDataInputStream(stream))

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

    private def createTypeInputStream(input: LittleEndianDataInputStream): Option[TypeInputStream[T]] = m match {
      case x if x <:< reflect.classTag[Byte] => Some(ByteInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Int] => Some(IntInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Long] => Some(LongInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Float] => Some(FloatInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Short] => Some(ShortInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[String] => Some(StringInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
    }
  }
}
