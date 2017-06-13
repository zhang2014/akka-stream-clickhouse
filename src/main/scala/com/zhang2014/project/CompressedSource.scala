package com.zhang2014.project

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream.{Outlet, Attributes, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{OutHandler, GraphStageLogic, GraphStage}
import com.zhang2014.project.misc.CompressedFactory

object CompressedSource
{
  def apply(file: String): Source[ByteBuffer, NotUsed] = Source
    .fromGraph(new CompressedSource(new RandomAccessFile(file, "r").getChannel))

  def apply(fileChannel: FileChannel, limit: Long = -1): Source[ByteBuffer, NotUsed] = Source
    .fromGraph(new CompressedSource(fileChannel))

  final class CompressedSource(bin: FileChannel, limit: Long = -1) extends GraphStage[SourceShape[ByteBuffer]]
  {
    private val compressedHeaderBuffer = ByteBuffer.allocate(17)

    val out   = Outlet[ByteBuffer]("CompressedSource-in")
    val shape = SourceShape.of[ByteBuffer](out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            bin.position() match {
              case pos if limit > 0 && pos < limit => push(out, nextCompressedBlock())
              case pos if bin.position() < bin.size => push(out, nextCompressedBlock())
              case _ => completeStage()
            }
          }
        }
      )
    }

    private def nextCompressedBlock(): ByteBuffer = {
      compressedHeaderBuffer.clear()
      bin.read(compressedHeaderBuffer)
      compressedHeaderBuffer.flip()

      compressedHeaderBuffer.getLong()
      compressedHeaderBuffer.getLong()
      CompressedFactory.get(compressedHeaderBuffer.get()).read(bin)
    }
  }
}


