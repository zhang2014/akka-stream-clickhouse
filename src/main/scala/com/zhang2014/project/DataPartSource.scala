package com.zhang2014.project

import java.io.{BufferedReader, FileReader}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}


object DataPartSource
{

  case class Record(props: List[(String, String, Any)])

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

  final class DataPartSource(home: String)(implicit system: ActorSystem, materializer: Materializer)
  {
    lazy val reader = new BufferedReader(new FileReader(s"$home/columns.txt"))

    val "columns format version: 1"    = reader.readLine()
    val Array(columnCount, "columns:") = reader.readLine().split(" ", 2)

    def createColumnsSource(): Seq[Source[(String, String, Any), NotUsed]] = {
      (0 until columnCount.toInt).map(_ => reader.readLine().split(" ", 2)).map {
        case Array(name, tpe) =>
          val resolvedName: String = resolveColumnName(name)
          ColumnSource[Any](tpe, resolveDataFile(resolvedName))
            .map[(String, String, Any)](v => (tpe, resolvedName, v))
      }
    }

    private def resolveDataFile(columnName: String) = s"$home/$columnName.bin"

    private def resolveColumnName(columnName: String) = columnName.substring(1, columnName.length - 1)

  }


  final case class RecordZip(n: Int) extends GraphStage[UniformFanInShape[(String, String, Any), Record]]
  {
    var pending      = 0
    var willShutDown = false


    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      inlets.foreach { in =>
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
            pending += inlets.length
            if (pending == 0) tryPushAll()
          }
        }
      )

      override def preStart(): Unit = inlets.foreach(pull)

      private def tryPushAll(): Unit = {
        push(out, Record(inlets.map(grab).toList))
        if (willShutDown) {
          completeStage()
        }
        else {
          inlets.foreach(pull)
        }
      }
    }


    lazy val inlets = (0 until n).map(i => Inlet[(String, String, Any)](s"Zip-Record-in-$i"))
    lazy val out    = Outlet[Record]("Zip-Record-out")
    lazy val shape  = UniformFanInShape(out, inlets: _*)
  }

}
