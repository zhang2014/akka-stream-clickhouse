package com.zhang2014.project.source

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{OutHandler, GraphStageLogic, GraphStage}
import com.zhang2014.project.misc.{CompressedFactory, CompressedRange, Range, UnCompressedRange}

object FileSource
{
  def apply(file: String, range: Range): Source[ByteBuffer, NotUsed] = Source
    .fromGraph(new FileSource(range, new RandomAccessFile(file, "r").getChannel))

  final class FileSource(range: Range, channel: FileChannel) extends GraphStage[SourceShape[ByteBuffer]]
  {

    private val compressedHead          = ByteBuffer.allocate(17)
    private val unCompressionBufferSize = 8192

    lazy val out   = shape.out
    lazy val shape = SourceShape.of[ByteBuffer](Outlet[ByteBuffer]("FileSource-in"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      channel.position(range.begin)
      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            remain(range) -> range match {
              case (size, rg: CompressedRange) if size > 0 => push(out, nextCompressedBlock(rg))
              case (size, rg: UnCompressedRange) if size > 0 => push(out, nextUnCompressedBlock(size))
              case _ => completeStage()
            }
          }
        }
      )
    }

    private def remain(rg: Range) = rg match {
      case _ if rg.end >= 0 => Math.min(channel.size(), rg.end) - channel.position()
      case _ => channel.size() - channel.position()
    }

    private def nextCompressedBlock(rg: CompressedRange) = rg match {
      case CompressedRange(begin, _, pos, _) if begin == channel.position() => val bf = readCompressedData; bf.position(pos.toInt); bf
      case CompressedRange(_, end, _, limit) if limit <= 0 => readCompressedData
      case CompressedRange(_, end, _, limit) => readCompressedData match {
        case buffer if end <= channel.position() => buffer.limit(limit.toInt); buffer
        case buffer => buffer
      }
    }

    private def nextUnCompressedBlock(remainCapacity: Long) = {
      val readSize = Math.min(unCompressionBufferSize, remainCapacity)
      val readBuffer = ByteBuffer.allocate(readSize.toInt)
      channel.read(readBuffer)
      readBuffer.flip()
      readBuffer
    }

    private def readCompressedData = {
      compressedHead.clear()
      channel.read(compressedHead)
      compressedHead.flip()

      compressedHead.getLong()
      compressedHead.getLong()
      CompressedFactory.get(compressedHead.get()).read(channel)
    }
  }

}
