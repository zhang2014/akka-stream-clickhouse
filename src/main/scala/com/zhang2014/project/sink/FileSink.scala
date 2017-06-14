package com.zhang2014.project.sink

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.misc.{CompressedFactory, CompressedRange, Range, UnCompressedRange}

object FileSink
{
  def apply(file: String, range: Option[Range], compression: Boolean = true): Sink[ByteBuffer, NotUsed] = {
    val channel = new RandomAccessFile(file, "w").getChannel
    Sink.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        range match {
          case None => createWriteSink(compression, channel).shape
          case Some(rg) =>
            val to = new RandomAccessFile(s"${file}_merge", "w").getChannel
            val branch = new RandomAccessFile(s"${file}_branch", "rw").getChannel
            val manager = b.add(RangeManager(mergeTask(rg, channel, branch, to)))
            manager ~> createWriteSink(compression, branch)
            SinkShape.of(manager.in)
        }
      }
    )
  }

  private def mergeTask(range: Range, origin: FileChannel, branch: FileChannel, merged: FileChannel) = {
    lazy val buffer = ByteBuffer.allocate(10 * 1024 * 1024)

    def appendFromRead(from: FileChannel, position: Long, limit: Long, to: FileChannel) = {
      from.position(position)
      var remain = limit - position
      while (remain > 0) {
        remain -= (remain match {
          case capacity if capacity > buffer.capacity() => from.read(buffer)
          case capacity => buffer.limit(capacity.toInt); from.read(buffer)
        })
        buffer.flip()
        merged.write(buffer)
        buffer.clear()
      }
    }

    range match {
      case rg: CompressedRange => () => {}
      case rg: UnCompressedRange => () => {
        appendFromRead(origin, 0, rg.begin, merged)
        appendFromRead(branch, 0, branch.size(), merged)
        appendFromRead(origin, rg.end, origin.size(), merged)
      }
    }
  }

  private def createWriteSink(compression: Boolean, channel: FileChannel) = compression match {
    case true => Sink.foreach[ByteBuffer](bf => CompressedFactory.get(0x80.toByte).write(channel, bf))
    case false => Sink.foreach[ByteBuffer](bf => channel.write(bf))
  }

  case class RangeManager(mergeHandler: () => Unit) extends GraphStage[FlowShape[ByteBuffer, ByteBuffer]]
  {
    lazy val in    = shape.in
    lazy val out   = shape.out
    lazy val shape = FlowShape.of(Inlet[ByteBuffer]("RangeManager-in"), Outlet[ByteBuffer]("RangeManager-out"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var finish = false
      setHandlers(
        in, out, new InHandler with OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = push(out, grab(in))

          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = finish match {
            case true => mergeHandler.apply()
            case false => tryPull(in)
          }

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = finish = true
        }
      )
    }
  }

}
