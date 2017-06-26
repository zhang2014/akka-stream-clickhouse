package com.zhang2014.project.source

import java.io.RandomAccessFile
import java.nio.{ByteOrder, ByteBuffer}
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
          override def onPull(): Unit = remaining(range) -> range match {
            case (`maxReadSize`, rg: CompressedRange) => push(out, readBlock(range, offset = rg.unCompressedBegin.toInt))
            case (0, rg: CompressedRange) if rg.unCompressedEnd > 0 => push(out, readBlock(range, 0, rg.unCompressedEnd.toInt))
            case (remain, rg) if remain > 0 => push(out, readBlock(rg))
            case _ => completeStage()
          }
        }
      )
    }

    private lazy val maxReadSize = range match {
      case _ if range.end > 0 => Math.min(channel.size(), range.end) - range.begin
      case _ => channel.size() - range.begin
    }

    private def remaining(range: Range) = range match {
      case _ if range.end >= 0 => Math.min(channel.size(), range.end) - channel.position()
      case _ => channel.size() - channel.position()
    }

    private def readBlock(range: Range, offset: Int = 0, limit: Int = -1) = {
      val nextBlock = range match {
        case rg: CompressedRange => readCompressedData
        case rg: UnCompressedRange if rg.end > 0 => readUncompressedData(rg.end - channel.position())
        case rg: UnCompressedRange => readUncompressedData(channel.size() - channel.position())
      }
      nextBlock.position(offset)
      if (limit > 0) nextBlock.limit(limit)
      nextBlock
    }

    private def readUncompressedData(remainCapacity: Long) = {
      val readSize = Math.min(unCompressionBufferSize, remainCapacity)
      val readBuffer = ByteBuffer.allocate(readSize.toInt).order(ByteOrder.LITTLE_ENDIAN)
      channel.read(readBuffer)
      readBuffer.flip()
      readBuffer
    }

    private def readCompressedData = {
      compressedHead.clear()
      channel.read(compressedHead)
      compressedHead.flip()
      val checkSum = Array(compressedHead.getLong(), compressedHead.getLong())
      CompressedFactory.get(compressedHead.get()).read(channel).order(ByteOrder.LITTLE_ENDIAN)
    }
  }
}
