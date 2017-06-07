package com.zhang2014.project

import java.io.{BufferedReader, FileReader}
import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Calendar, Date}

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}



object DataPartSource
{
  def apply(path: String): Source[Record, NotUsed] = {
    //TODO:验证CheckSum
    //TODO:考虑primary.idx主键索引的使用
    val reader = new BufferedReader(new FileReader(s"$path/columns.txt"))

    val "columns format version: 1" = reader.readLine()
    val Array(columnCount, "columns:") = reader.readLine().split(" ", 2)

    import DataType._
    Source.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val columns = (0 until columnCount.toInt).map(_ => reader.readLine().split(" ", 2)).map(removeIgnoreChar).map {
          case Array(columnName, "Date") => readDate(path, columnName).map(v => (DATE, columnName, v))
          case Array(columnName, "Int16") => readInt16(path, columnName).map(v => (INT16, columnName, v))
          case Array(columnName, "Int32") => readInt32(path, columnName).map(v => (INT32, columnName, v))
          case Array(columnName, "Int64") => readInt64(path, columnName).map(v => (INT64, columnName, v))
          case Array(columnName, "UInt16") => readUInt16(path, columnName).map(v => (UINT16, columnName, v))
          case Array(columnName, "UInt32") => readUInt32(path, columnName).map(v => (UINT32, columnName, v))
          case Array(columnName, "UInt64") => readUInt64(path, columnName).map(v => (UINT64, columnName, v))
          case Array(columnName, "String") => ColumnSource[String](path, columnName).map(v => (STRING, columnName, v))
        }
        val zip = b.add(RecordZip(columns.length))

        columns.zipWithIndex.foreach { case (s, i) => s ~> zip.in(i) }
        SourceShape(zip.out)
      }
    )
  }

  import scala.concurrent.duration._

  private def readDate(path: String, columnName: String) = readUInt16(path, columnName)
    .map(d => new Date(d.days.toMillis))

  private def readInt8(path: String, columnName: String) = ColumnSource[Byte](path, columnName)

  private def readUInt8(path: String, columnName: String) = readInt8(path, columnName).map(_ & 0x0FF)

  private def readInt16(path: String, columnName: String) = ColumnSource[Short](path, columnName)

  private def readUInt16(path: String, columnName: String) = readInt16(path, columnName).map(_ & 0x0FFFF)

  private def readInt32(path: String, columnName: String) = ColumnSource[Int](path, columnName)

  private def readUInt32(path: String, columnName: String) = readInt32(path, columnName).map(_ & 0x0FFFFFFFFL)

  private def readInt64(path: String, columnName: String) = ColumnSource[Long](path, columnName)

  private val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)

  private def readUInt64(path: String, columnName: String) = readInt64(path, columnName).map { l =>
    longBuffer.clear()
    longBuffer.putLong(l)
    BigInt(new BigInteger(1, longBuffer.array()))
  }

  private def removeIgnoreChar(arr: Array[String]) = {
    arr(0) = arr(0).substring(1, arr(0).length - 1)
    arr
  }

  private final case class RecordZip(n: Int) extends GraphStage[UniformFanInShape[(Byte, String, Any), Record]]
  {
    var props        = List.empty[(Byte, String, Any)]
    var pending      = 0
    var willShutDown = false


    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private def tryPushAll(): Unit = {
        push(out, Record(ins.map(grab).toList))
        if (willShutDown) {
          completeStage()
        }
        else {
          ins.foreach(pull)
        }
      }

      ins.foreach { in =>
        setHandler(
          in, new InHandler
          {
            @throws[Exception](classOf[Exception])
            override def onPush(): Unit = {
              pending -= 1
              if (pending == 0) tryPushAll()
            }

            @throws[Exception](classOf[Exception])
            override def onUpstreamFinish(): Unit = {
              if (!isAvailable(in)) completeStage()
              willShutDown = true
            }
          }
        )
      }

      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = {
            pending += ins.length
            if (pending == 0) tryPushAll()
          }
        }
      )

      override def preStart(): Unit = ins.foreach(pull)
    }


    val ins   = (0 until n).map(i => Inlet[(Byte, String, Any)](s"Zip-Record-in-$i"))
    val out   = Outlet[Record]("Zip-Record-out")
    val shape = UniformFanInShape(out, ins: _*)
  }

}
