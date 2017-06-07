package com.zhang2014.project

import java.io.{DataInputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.google.common.io.LittleEndianDataInputStream

import scala.collection.JavaConverters
import scala.reflect.ClassTag

object ColumnSource
{
  def apply[T: ClassTag](pathWithPart: String, columnName: String): Source[T, NotUsed] = {
    val source = new ColumnSource[T](
      new RandomAccessFile(pathWithPart + "/" + columnName + ".bin", "r").getChannel,
      new RandomAccessFile(pathWithPart + "/" + columnName + ".mrk", "r").getChannel
    )
    Source.fromGraph[T, NotUsed](source)
  }

}

final class ColumnSource[T](bin: FileChannel, mark: FileChannel)(implicit m: ClassTag[T])
  extends GraphStage[SourceShape[T]]
{
  lazy val out   = shape.out
  lazy val shape = SourceShape(Outlet[T]("ClickHouse-Column-Source"))

  val markReader     = MarkReader(mark)
  var dataBlockChunk = Option.empty[TypeInputStream[T]]

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(
      out, new OutHandler
      {
        @throws[Exception](classOf[Exception])
        override def onPull(): Unit = {
          var ele = dataBlockChunk.flatMap(_.next)
          if (ele.isEmpty) {
            dataBlockChunk = loadBlockData()
            ele = dataBlockChunk.flatMap(_.next)
          }
          ele match {
            case None => completeStage()
            case Some(element) => push(out, element)
          }
        }
      }
    )
  }

  private def loadBlockData(): Option[TypeInputStream[T]] = {
    def fun(t: (Int, Long, Long)): (Boolean, ByteBuffer) = t match {
      case (0, position, offset) => (true, readCompressedData(position, offset.toInt, -1))
      case (i, position, 0) => (false, ByteBuffer.allocate(0))
      case (i, position, offset) =>
        val buffer = readCompressedData(position, 0, -1)
        (buffer.limit() != offset, buffer.limit(offset.toInt).asInstanceOf[ByteBuffer])
    }
    import JavaConverters._
    markReader.readWith(Function.untupled(fun)) match {
      case Nil => Option.empty[TypeInputStream[T]]
      case buffers => createTypeInputStream(new LittleEndianDataInputStream(new ByteBufferInputStream(buffers.asJava)))
    }
  }

  private val compressedHeaderBuffer = ByteBuffer.allocate(17)

  private def readCompressedData(position: Long, offset: Int, limit: Int) = {
    bin.position(position)
    compressedHeaderBuffer.clear()
    bin.read(compressedHeaderBuffer)
    compressedHeaderBuffer.flip()

    compressedHeaderBuffer.getLong()
    compressedHeaderBuffer.getLong()
    CompressedFactory.get(compressedHeaderBuffer.get()).read(bin, offset, limit)
  }

  private def createTypeInputStream(input: LittleEndianDataInputStream): Option[TypeInputStream[T]] =
    m match {
      case x if x <:< reflect.classTag[Byte] => Some(ByteInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Int] => Some(IntInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Long] => Some(LongInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Float] => Some(FloatInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[Float] => Some(FloatInputSteam(input)).map(_.asInstanceOf[TypeInputStream[T]])
      case x if x <:< reflect.classTag[String] => Some(StringInputStream(input)).map(_.asInstanceOf[TypeInputStream[T]])
    }

}
