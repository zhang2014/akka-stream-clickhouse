package com.zhang2014.project

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream.{Outlet, Attributes, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{OutHandler, GraphStageLogic, GraphStage}
import com.zhang2014.project.misc.CompressedFactory

/**
  * 用于按块从文件中读取被压缩的数据,目前只支持LZ4的压缩
  */
object CompressedSource
{
  def apply(file: String): Source[ByteBuffer, NotUsed] = Source
    .fromGraph(new CompressedSource(new RandomAccessFile(file, "r").getChannel))

  def apply(fileChannel: FileChannel): Source[ByteBuffer, NotUsed] = Source.fromGraph(new CompressedSource(fileChannel))

  private final class CompressedSource(bin: FileChannel) extends GraphStage[SourceShape[ByteBuffer]]
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
            if (bin.position() < bin.size()) {
              push(out, nextCompressedBlock())
            } else {
              completeStage()
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


