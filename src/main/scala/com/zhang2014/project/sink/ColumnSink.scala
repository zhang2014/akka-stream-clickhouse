package com.zhang2014.project.sink

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.misc.{CompressedRange, Range, WriteUtility}

object ColumnSink
{
  def apply[T](dataType: String, file: String, range: Range = CompressedRange(0, -1, 0, -1)): Sink[T, NotUsed] =
    Flow.fromGraph(TransForm[T](dataType, 0)).to(FileSink(file, range))

  final case class TransForm[T](dataType: String, blockSize: Int) extends GraphStage[FlowShape[T, ByteBuffer]]
  {
    lazy val in    = shape.in
    lazy val out   = shape.out
    lazy val shape = FlowShape.of(Inlet[T]("TransForm-in"), Outlet[ByteBuffer]("TransForm-out"))

    var count  = 0
    var buffer = ByteBuffer.allocate(10 * 1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandlers(
        in, out, new InHandler with OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            count -= 1
            pushToBuffer[T](dataType, grab(in))
            count match {
              case 0 => push(out, buffer)
              case pos => tryPull(in)
            }
          }

          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            count += blockSize
            buffer.clear()
            tryPull(in)
          }
        }
      )

      private def pushToBuffer[A](tpe: String, value: A): Unit = tpe match {
        case "Date" => pushToBuffer[Int]("UInt16", TimeUnit.DAYS.toDays(value.asInstanceOf[Date].getTime).toInt)
        case "Int8" => buffer = WriteUtility.maybeResize(buffer, 1).put(value.asInstanceOf[Byte])
        case "Int32" => buffer = WriteUtility.maybeResize(buffer, 4).putInt(value.asInstanceOf[Byte])
        case "Int64" => buffer = WriteUtility.maybeResize(buffer, 8).putLong(value.asInstanceOf[Long])
        case "Int16" => buffer = WriteUtility.maybeResize(buffer, 2).putShort(value.asInstanceOf[Short])
        case "Float32" => buffer = WriteUtility.maybeResize(buffer, 4).putFloat(value.asInstanceOf[Float])
        case "VarInt" => buffer = WriteUtility.writeVarInt(buffer, value.asInstanceOf[Long])
        case "UInt16" => buffer = WriteUtility.maybeResize(buffer, 2).putShort(value.asInstanceOf[Int].toShort)
        case "UInt64" => buffer = WriteUtility.maybeResize(buffer, 8).put(value.asInstanceOf[BigInt].toByteArray)
        case "String" => buffer = WriteUtility.writeUTF(buffer, value.asInstanceOf[String])
        case _ if tpe.startsWith("Tuple(") =>
          val product = value.asInstanceOf[Product]
          tpe.substring(6, tpe.length - 1).split(",").zipWithIndex.foreach {
            case (subType, idx) => pushToBuffer(subType, product.productElement(idx))
          }
      }
    }
  }

}
