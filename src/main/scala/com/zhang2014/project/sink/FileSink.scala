package com.zhang2014.project.sink

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.ext.CompressionBuffer.CompressionBuffer
import akka.stream.Attributes.Attribute
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import com.zhang2014.project.misc.{UnCompressedRange, CompressedFactory, CompressedRange, Range}

object FileSink
{

  def apply(file: String, range: Range): Sink[ByteBuffer, NotUsed] = {
    val origin = new RandomAccessFile(file, "r").getChannel
    val branch = new RandomAccessFile(s"${file}_tmp", "w").getChannel

    copyData(origin, branch, 0, range.begin)
    Sink.fromGraph(
      FileSink(origin, branch, range) {
        range match {
          case rg: CompressedRange if rg.end > 0 => copyData(origin, branch, skip(origin, range.end), origin.size())
          case rg: UnCompressedRange if rg.end > 0 => copyData(origin, branch, range.end, origin.size())
        }
        new File(s"${file}_tmp").renameTo(new File(file))
      }
    )
  }

  private def copyData(from: FileChannel, to: FileChannel, begin: Long, end: Long): Unit = {
    from.position(begin)
    var remain = Math.max(0, end)
    val buffer = ByteBuffer.allocate(10 * 1024 * 1024)
    while (remain > 0) {
      buffer.limit(Math.min(remain, buffer.capacity()).toInt)
      remain -= from.read(buffer)
      buffer.flip()
      to.write(buffer)
    }
    to.force(true)
  }

  private def skip(channel: FileChannel, pos: Long): Long = {
    channel.position(pos)
    val compressedHead = ByteBuffer.allocate(17)
    channel.read(compressedHead)
    CompressedFactory.get(compressedHead.get(16)).read(channel)
    channel.position()
  }

  case class FileSink(origin: FileChannel, branch: FileChannel, range: Range)(onComplete: => Unit)
    extends GraphStage[SinkShape[ByteBuffer]] with Attribute
  {
    lazy val in    = shape.in
    lazy val shape = SinkShape.of(Inlet[ByteBuffer]("FileSink-in"))

    override def createLogic(attributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      val CompressionBuffer(min, buffer) = attributes.get(CompressionBuffer(65535, ByteBuffer.allocate(1048576)))

      setHandler(
        in, new InHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = buffer match {
            case bf if bf.position() > 0 => copyAndWrite(grab(in)); tryPull(in)
            case bf => writeData(branch, bf); buffer.clear(); tryPull(in)
          }

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = {
            readTruncatedDataWithAfter() match {
              case None if buffer.position() > 0 => writeData(branch, buffer)
              case Some(bf) => copyAndWrite(bf) if (buffer.position() > 0) writeData(branch, buffer)
              case _ =>
            }
            onComplete
            completeStage()
          }
        }
      )

      override def preStart(): Unit = readTruncatedDataWithBefore() match {
        case None => tryPull(in)
        case Some(truncatedData) => copyAndWrite(truncatedData); tryPull(in)
      }

      private def readTruncatedDataWithBefore() = {
        Option(range).filter(_.isInstanceOf[CompressedRange]).flatMap {
          case rg: CompressedRange if rg.unCompressedBegin > 0 =>
            origin.position(rg.begin)
            val compressedHead = ByteBuffer.allocate(17)
            origin.read(compressedHead)
            val unCompressedBuffer = CompressedFactory.get(compressedHead.get(16)).read(origin)
            unCompressedBuffer.limit(rg.unCompressedBegin.toInt)
            Some(unCompressedBuffer)
          case _ => None
        }
      }

      private def readTruncatedDataWithAfter() = {
        Option(range).filter(_.isInstanceOf[CompressedRange]).flatMap {
          case rg: CompressedRange if rg.unCompressedEnd > 0 =>
            origin.position(rg.end)
            val compressedHead = ByteBuffer.allocate(17)
            origin.read(compressedHead)
            val unCompressedBuffer = CompressedFactory.get(compressedHead.get(16)).read(origin)
            unCompressedBuffer.position(rg.unCompressedEnd.toInt)
            Some(unCompressedBuffer)
          case _ => None
        }
      }

      private def copyAndWrite(from: ByteBuffer) = {
        while (from.hasRemaining) {
          buffer.put(from.get())
          if (!buffer.hasRemaining) {
            writeData(branch, buffer)
            buffer.clear()
          }
        }
      }

      private def writeData(channel: FileChannel, bf: ByteBuffer): Unit = range match {
        case rg: CompressedRange => CompressedFactory.get(0x80.toByte).write(channel, bf)
        case _ => channel.write(bf)
      }
    }
  }
}
