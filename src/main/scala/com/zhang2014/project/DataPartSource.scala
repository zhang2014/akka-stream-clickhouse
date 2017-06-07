package com.zhang2014.project

import java.io.{BufferedReader, FileReader}
import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

case class Record(props: List[(Byte, String, Any)])

object DataPartSource
{
  def apply(path: String): Source[Record, NotUsed] = {
    //TODO:验证CheckSum
    //TODO:考虑primary.idx主键索引的使用
    val reader = new BufferedReader(new FileReader(s"$path/columns.txt"))

    val "columns format version: 1" = reader.readLine()
    val Array(columnCount, "columns:") = reader.readLine().split(" ", 2)

    Source.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val columns = (0 until columnCount.toInt).map(_ => reader.readLine().split(" ", 2)).map {
          case Array(columnName, "Date") => readUInt16(path, columnName).map(createDate)
            .map(v => (1.toByte, columnName, v))
          case Array(columnName, "Int16") => readInt16(path, columnName).map(v => (2.toByte, columnName, v))
          case Array(columnName, "Int32") => readInt32(path, columnName).map(v => (3.toByte, columnName, v))
          case Array(columnName, "Int64") => readInt64(path, columnName).map(v => (4.toByte, columnName, v))
          case Array(columnName, "UInt16") => readUInt16(path, columnName).map(v => (5.toByte, columnName, v))
          case Array(columnName, "UInt32") => readUInt32(path, columnName).map(v => (6.toByte, columnName, v))
          case Array(columnName, "UInt64") => readUInt64(path, columnName).map(v => (7.toByte, columnName, v))
        }
        val zip = b.add(RecordZip(columns.length))

        columns.zipWithIndex.foreach { case (s, i) => s ~> zip.in(i) }
        SourceShape(zip.out)
      }
    )
  }

  private def createDate(day: Int): Date = ???

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

    }


    val ins   = (0 until n).map(i => Inlet[(Byte, String, Any)](s"Zip-Record-in-$i"))
    val out   = Outlet[Record]("Zip-Record-out")
    val shape = UniformFanInShape(out, ins: _*)
  }

}
