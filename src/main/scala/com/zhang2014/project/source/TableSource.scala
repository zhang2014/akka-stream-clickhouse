package com.zhang2014.project.source

import java.io.File
import java.text.SimpleDateFormat

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.zhang2014.project.source.DataPartSource.Record

import scala.concurrent.duration._

object TableSource
{

  object DataPart
  {
    def apply(path: String, fullPath: String): DataPart = {
      val Array(minDayStr, maxDayStr, minBlockStr, maxBlockStr, level) = path.split("\\_")
      val format = new SimpleDateFormat("YYYYMMDD")
      val minDay = format.parse(minDayStr).getTime.millis.toDays
      val maxDay = format.parse(maxDayStr).getTime.millis.toDays
      new DataPart(minDay.toInt, maxDay.toInt, minBlockStr.toInt, maxBlockStr.toInt, level.toInt, fullPath)
    }
  }

  case class DataPart(
    minMonth: Int, maxMonth: Int, minBlockNumber: Int, maxBlockNumber: Int, level: Int, fullPath: String
  )

  def apply(path: String)(implicit system: ActorSystem, materializer: Materializer): Source[Record, NotUsed] = {
    val tableDataPath = new File(path)
    if (!tableDataPath.exists() || !tableDataPath.isDirectory) {
      throw new IllegalArgumentException("Table Path should is a directory")
    }

    Source.fromGraph(
      GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        val parts = tableDataPath.listFiles().filter(_.getName != "detached")
          .map(f => DataPart.apply(f.getName, f.getAbsolutePath))
          .sortWith((l, r) => l.minMonth < r.minMonth || l.minBlockNumber < r.minBlockNumber)
          .map(p => DataPartSource.apply(p.fullPath))

        val union = b.add(UnionSource(parts.length))

        parts.zipWithIndex.foreach { case (s, idx) => s ~> union.in(idx) }

        SourceShape[Record](union.out)
      }
    )
  }

  private final case class UnionSource(n: Int) extends GraphStage[UniformFanInShape[Record, Record]]
  {
    val out    = Outlet[Record]("Union-Source-out")
    val inlets = (0 until n).map(i => Inlet[Record](s"Union-Source-in-$i"))
    val shape  = UniformFanInShape(out, inlets: _*)

    var pos = 0

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      setHandler(
        out, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = tryPull(inlets(pos))
        }
      )
      inlets.zipWithIndex.foreach { case (in, idx) =>
        setHandler(
          in, new InHandler
          {
            @throws[Exception](classOf[Exception])
            override def onPush(): Unit = push(out, grab(in))

            @throws[Exception](classOf[Exception])
            override def onUpstreamFinish(): Unit = {
              pos = idx + 1
              if (pos == n) {
                completeStage()
              }
              else {
                tryPull(inlets(pos))
              }
            }
          }
        )
      }
    }

  }

}
