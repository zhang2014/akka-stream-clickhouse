package com.zhang2014.project

import java.io.{BufferedReader, FileReader}
import java.math.BigInteger
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.DataType._


object DataPartSource
{

  case class Record(props: List[(Byte, String, Any)])

  def apply(path: String)(implicit system: ActorSystem, materializer: Materializer): Source[Record, NotUsed] = {
    Source.fromGraph(
      g = GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val columns = new DataPartSource(path).createColumnsSource()
        val zip = b.add(RecordZip(columns.length))

        columns.zipWithIndex.foreach { case (s, i) => s ~> zip.in(i) }
        SourceShape(zip.out)
      }
    )
  }

  private final class DataPartSource(path: String)(implicit system: ActorSystem, materializer: Materializer)
  {

    import scala.concurrent.duration._

    val reader                         = new BufferedReader(new FileReader(s"$path/columns.txt"))
    val "columns format version: 1"    = reader.readLine()
    val Array(columnCount, "columns:") = reader.readLine().split(" ", 2)

    def createColumnsSource() = (0 until columnCount.toInt).map(_ => reader.readLine().split(" ", 2))
      .map(removeIgnoreChar).map {
      case Array(columnName, "Date") => readDate(resolveDataFile(columnName)).map(v => (DATE, columnName, v))
      case Array(columnName, "Int16") => readInt16(resolveDataFile(columnName)).map(v => (INT16, columnName, v))
      case Array(columnName, "Int32") => readInt32(resolveDataFile(columnName)).map(v => (INT32, columnName, v))
      case Array(columnName, "Int64") => readInt64(resolveDataFile(columnName)).map(v => (INT64, columnName, v))
      case Array(columnName, "UInt16") => readUInt16(resolveDataFile(columnName)).map(v => (UINT16, columnName, v))
      case Array(columnName, "UInt32") => readUInt32(resolveDataFile(columnName)).map(v => (UINT32, columnName, v))
      case Array(columnName, "UInt64") => readUInt64(resolveDataFile(columnName)).map(v => (UINT64, columnName, v))
      case Array(columnName, "String") => ColumnSource[String](resolveDataFile(columnName))
        .map(v => (STRING, columnName, v))
    }

    private def resolveDataFile(columnName: String) = s"$path/$columnName.bin"

    private def readDate(dataFile: String) = readUInt16(dataFile).map(d => new Date(d.days.toMillis))

    private def readInt8(dataFile: String) = ColumnSource[Byte](dataFile)

    private def readUInt8(dataFile: String) = readInt8(dataFile).map(_ & 0x0FF)

    private def readInt16(dataFile: String) = ColumnSource[Short](dataFile)

    private def readUInt16(dataFile: String) = readInt16(dataFile).map(_ & 0x0FFFF)

    private def readInt32(dataFile: String) = ColumnSource[Int](dataFile)

    private def readUInt32(dataFile: String) = readInt32(dataFile).map(_ & 0x0FFFFFFFFL)

    private def readInt64(dataFile: String) = ColumnSource[Long](dataFile)

    private def readUInt64(dataFile: String) = {
      val longBuffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN)
      readInt64(dataFile).map { l =>
        longBuffer.clear()
        longBuffer.putLong(l)
        BigInt(new BigInteger(1, longBuffer.array()))
      }
    }

    private def removeIgnoreChar(arr: Array[String]) = arr match {
      case Array(typ, columnName) => Array(typ, columnName.substring(1, columnName.length - 1))
    }
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
