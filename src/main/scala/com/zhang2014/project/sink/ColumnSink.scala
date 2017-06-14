package com.zhang2014.project.sink

import java.nio.ByteBuffer
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.misc.{Range, WriteUtility}

object ColumnSink
{
  def apply[T](dataType: String, file: String): Sink[T, NotUsed] = apply(dataType, file, None)

  def apply[T](dataType: String, file: String, overWriteRange: Option[Range] = None): Sink[T, NotUsed] = ???

  final case class TransForm[T](dataType: String, compressThreshold: Int) extends GraphStage[FlowShape[T, ByteBuffer]]
  {
    lazy val in    = shape.in
    lazy val out   = shape.out
    lazy val shape = FlowShape.of(Inlet[T]("TransForm-in"), Outlet[ByteBuffer]("TransForm-out"))

    var buffer = ByteBuffer.allocate(compressThreshold * 20)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandlers(
        in, out, new InHandler with OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            tryPushNext[T](dataType, grab(in))
            buffer.position() match {
              case pos if pos > compressThreshold => buffer.flip(); push(out, buffer)
              case _ => tryPull(in)
            }
          }

          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            buffer.clear()
            tryPull(in)
          }
        }
      )

      private def tryPushNext[A](tpe: String, value: A): Unit = tpe match {
        case "Date" => tryPushNext[Int]("UInt16", TimeUnit.DAYS.toDays(value.asInstanceOf[Date].getTime).toInt)
        case "Int8" => buffer.put(value.asInstanceOf[Byte])
        case "Int32" => buffer.putInt(value.asInstanceOf[Byte])
        case "Int64" => buffer.putLong(value.asInstanceOf[Long])
        case "Int16" => buffer.putShort(value.asInstanceOf[Short])
        case "Float" => buffer.putFloat(value.asInstanceOf[Float])
        case "VarInt" => WriteUtility.writeVarInt(buffer, value.asInstanceOf[Long])
        case "UInt16" => buffer.putShort(value.asInstanceOf[Int].toShort)
        case "UInt64" => buffer.put(value.asInstanceOf[BigInt].toByteArray)
        case "String" => WriteUtility.writeUTF(buffer, value.asInstanceOf[String])
        case _ if tpe.startsWith("Tuple(") =>
          val product = value.asInstanceOf[Product]
          tpe.substring(6, tpe.length - 1).split(",").zipWithIndex.foreach {
            case (subType, idx) => tryPushNext(subType, product.productElement(idx))
          }
      }
    }
  }

}
