package com.zhang2014.project.sink

import java.nio.{ByteOrder, ByteBuffer}
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.ext.WriteSettings.WriteSettings
import akka.stream._
import akka.stream.scaladsl.{Keep, Flow, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.misc.{CompressedRange, Range, WriteUtility}

import scala.concurrent.Future

object ColumnSink
{
  def apply[T](dataType: String, file: String, range: Range = CompressedRange(0, -1, 0, -1)): Sink[T, Future[Done]] =
    Flow.fromGraph(TransForm[T](dataType)).toMat(FileSink(file, range))(Keep.right)

  final case class TransForm[T](dataType: String) extends GraphStage[FlowShape[T, ByteBuffer]]
  {
    lazy val in    = shape.in
    lazy val out   = shape.out
    lazy val shape = FlowShape.of(Inlet[T]("TransForm-in"), Outlet[ByteBuffer]("TransForm-out"))

    override def createLogic(attributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      var count       = 0
      val settings    = attributes.get[WriteSettings](WriteSettings())
      var batchBuffer = ByteBuffer.allocate(settings.blockSize * dataTypeAvg(dataType)).order(ByteOrder.LITTLE_ENDIAN)

      setHandlers(
        in, out, new InHandler with OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            count match {
              case 1 => push(out, serialize(dataType, grab(in))); count -= 1
              case pos => batchBuffer = serialize[T](dataType, grab(in)); count -= 1; tryPull(in)
            }
          }

          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            count += settings.blockSize
            batchBuffer.clear()
            tryPull(in)
          }
        }
      )

      private def serialize[A](tpe: String, value: A): ByteBuffer = tpe match {
        case "Date" => serialize[Int]("UInt16", TimeUnit.DAYS.toDays(value.asInstanceOf[Date].getTime).toInt)
        case "Int8" => WriteUtility.maybeResize(batchBuffer, 1).put(value.asInstanceOf[Byte])
        case "Int32" => WriteUtility.maybeResize(batchBuffer, 4).putInt(value.asInstanceOf[Byte])
        case "Int64" => WriteUtility.maybeResize(batchBuffer, 8).putLong(value.asInstanceOf[Long])
        case "Int16" => WriteUtility.maybeResize(batchBuffer, 2).putShort(value.asInstanceOf[Short])
        case "Float32" => WriteUtility.maybeResize(batchBuffer, 4).putFloat(value.asInstanceOf[Float])
        case "Float64" => WriteUtility.maybeResize(batchBuffer, 8).putDouble(value.asInstanceOf[Double])
        case "VarInt" => WriteUtility.writeVarInt(batchBuffer, value.asInstanceOf[Long])
        case "UInt16" => WriteUtility.maybeResize(batchBuffer, 2).putShort(value.asInstanceOf[Int].toShort)
        case "UInt64" => WriteUtility.maybeResize(batchBuffer, 8).put(value.asInstanceOf[BigInt].toByteArray)
        case "String" => WriteUtility.writeUTF(batchBuffer, value.asInstanceOf[String])
        case _ if tpe.startsWith("Tuple(") =>
          val product = value.asInstanceOf[Product]
          tpe.substring(6, tpe.length - 1).split(",").zipWithIndex.map {
            case (subType, idx) => serialize(subType, product.productElement(idx))
          }.last
      }
    }

    private def dataTypeAvg(tpe: String): Int = tpe match {
      case "Date" => 2
      case "VarInt" => 4
      case "String" => 4 * 20
      case _ if tpe.startsWith("Int") => tpe.substring(3).toInt / 8
      case _ if tpe.startsWith("UInt") => tpe.substring(4).toInt / 8
      case _ if tpe.startsWith("Float") => tpe.substring(5).toInt / 8
      case _ if tpe.startsWith("Tuple(") => tpe.substring(6, tpe.length - 1).split(",").map(sub => dataTypeAvg(sub)).sum
    }
  }

}
