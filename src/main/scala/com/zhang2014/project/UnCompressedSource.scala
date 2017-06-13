package com.zhang2014.project

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream.stage.{OutHandler, GraphStage, GraphStageLogic}
import akka.stream.{Outlet, Attributes, SourceShape, Graph}
import akka.stream.scaladsl.Source

object UnCompressedSource
{
  def apply(file: String): Source[ByteBuffer, NotUsed] = apply(new RandomAccessFile(file, "r").getChannel)

  def apply(channel: FileChannel, limit: Long = -1): Source[ByteBuffer, NotUsed] = Source
    .fromGraph(new UnCompressedSource(channel))

  final class UnCompressedSource(channel: FileChannel, bufferSize: Int = 8192, limit: Long = -1)
    extends GraphStage[SourceShape[ByteBuffer]]
  {
    lazy val out   = shape.out
    lazy val shape = SourceShape.of[ByteBuffer](Outlet[ByteBuffer]("CompressedSource-in"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            channel.position() match {
              case pos if limit > 0 && pos < limit => push(out, fullyBuffer(limit - pos))
              case pos if pos < channel.size() => push(out, fullyBuffer(channel.size() - pos))
              case _ => completeStage()
            }
          }
        }
      )
    }

    private def fullyBuffer(maxReadSize: Long): ByteBuffer = {
      val capacity = maxReadSize match {
        case size if size < bufferSize => size
        case _ => bufferSize
      }
      val readBuffer = ByteBuffer.allocate(capacity.toInt)
      channel.read(readBuffer)
      readBuffer.flip()
      readBuffer
    }
  }

}
