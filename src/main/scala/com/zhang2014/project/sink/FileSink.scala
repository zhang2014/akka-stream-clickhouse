package com.zhang2014.project.sink

import java.io.{File, RandomAccessFile}
import java.nio.{ByteOrder, ByteBuffer}
import java.nio.channels.FileChannel

import akka.Done
import akka.ext.WriteSettings.WriteSettings
import akka.stream.Attributes.Attribute
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import com.zhang2014.project.misc._

import scala.concurrent.{Future, Promise}

object FileSink
{

  def apply(file: String, range: Range): Sink[ByteBuffer, Future[Done]] = {
    val origin = new RandomAccessFile(file, "r").getChannel
    val branch = new RandomAccessFile(s"${file}_tmp", "rw").getChannel

    val promise = Promise[Done]
    copyData(origin, branch, 0, range.begin)
    Sink.fromGraph(
      FileSink(origin, branch, range) {
        range match {
          case rg: CompressedRange if rg.end > 0 => copyData(origin, branch, skip(origin, range.end), origin.size())
          case rg: UnCompressedRange if rg.end > 0 => copyData(origin, branch, range.end, origin.size())
          case _ =>
        }
        origin.close()
        branch.close()
        new File(file).delete()
        new File(s"${file}_tmp").renameTo(new File(file))
        promise.success(Done)
      }
    ).mapMaterializedValue(_ => promise.future)
  }

  private def copyData(from: FileChannel, to: FileChannel, begin: Long, end: Long): Unit = {
    from.position(begin)
    var remain = Math.max(0, end - begin)
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

      val settings = attributes.get(WriteSettings(65535, 1048576))

      setHandler(
        in, new InHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = copyAndWrite(grab(in)) match {
            case pos if pos > settings.minInBytes => writeData(branch); tryPull(in)
            case _ => tryPull(in)
          }

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = {
            readTruncatedDataWithAfter().map(copyAndWrite).getOrElse(settings.uncompressedBuffer.position()) match {
              case pos if pos > 0 => writeData(branch); onComplete; completeStage()
              case _ => onComplete; completeStage()
            }
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
            Some(readCompressedData(rg.begin, bufLimit = rg.unCompressedBegin.toInt))
          case _ => None
        }
      }

      private def readTruncatedDataWithAfter() = {
        Option(range).filter(_.isInstanceOf[CompressedRange]).flatMap {
          case rg: CompressedRange if rg.unCompressedEnd > 0 =>
            Some(readCompressedData(rg.end, bufPos = rg.unCompressedEnd.toInt))
          case _ => None
        }
      }

      private def copyAndWrite(from: ByteBuffer) = {
        while (from.hasRemaining) {
          settings.uncompressedBuffer.put(from.get())
          if (!settings.uncompressedBuffer.hasRemaining) {
            writeData(branch)
            settings.uncompressedBuffer.clear()
          }
        }
        settings.uncompressedBuffer.position()
      }

      private val compressionHead = ByteBuffer.allocate(17).order(ByteOrder.LITTLE_ENDIAN)

      private def writeData(channel: FileChannel): Unit = {
        val bf = settings.uncompressedBuffer
        bf.flip()
        range match {
          case rg: CompressedRange =>
            val compressedData = CompressedFactory.get(0x82.toByte).compression(bf)
            val checkSum = CityHash.cityHash128(compressedData.array(), 0, compressedData.limit())
            compressionHead.clear()
            compressionHead.putLong(checkSum(0))
            compressionHead.putLong(checkSum(1))
            compressionHead.put(0x82.toByte)
            compressionHead.flip()
            settings.totalByteSize += branch.write(compressionHead)
            settings.totalByteSize += branch.write(compressedData)
          case _ => settings.totalByteSize += channel.write(bf)
        }
        channel.force(true)
      }
    }

    def readCompressedData(readPos: Long, bufPos: Int = 0, bufLimit: Int = -1): ByteBuffer = {
      val compressedHead = ByteBuffer.allocate(17)
      origin.position(readPos)
      origin.read(compressedHead)
      CompressedFactory.get(compressedHead.get(16)).read(origin) match {
        case buffer if bufLimit > 0 => buffer.position(bufPos); buffer.limit(bufLimit); buffer
        case buffer => buffer.position(bufPos); buffer
      }
    }
  }
}
