package akka.ext

import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}


case class ImplicitSource[T, MAT](source: Source[T, MAT])
{
  def mapDependsWindow[Out](size: Int)(map: Seq[T] => Out): Source[Out, MAT] = source.via(MapDependsWindow(size, map))

  def union(union: Source[T, _]): Source[T, MAT] = {
    source.via(
      Flow.fromGraph(
        GraphDSL.create(union) { implicit b => r =>
          import GraphDSL.Implicits._
          val gUnion = b.add(Union[T](2))
          r ~> gUnion.in(1)
          FlowShape.of(gUnion.in(0), gUnion.out)
        }
      )
    )
  }

  case class Union[Out](size: Int) extends GraphStage[UniformFanInShape[Out, Out]]
  {
    lazy val outlet = Outlet[Out]("Union-out")
    lazy val inlets = (0 until size).map(i => Inlet[Out](s"Union-in-$i"))

    lazy val shape = UniformFanInShape(outlet, inlets: _*)


    var pos = 0

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      inlets.foreach(
        in => setHandler(
          in, new InHandler
          {
            @throws[Exception](classOf[Exception])
            override def onPush(): Unit = push(outlet, grab(in))
          }
        )
      )
      setHandler(
        outlet, new OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = tryPull(inlets(pos))

          @throws[Exception](classOf[Exception])
          override def onDownstreamFinish(): Unit = { pos += 1; if (pos >= size) completeStage() }
        }
      )
    }
  }

  case class MapDependsWindow[In, Out](size: Int, map: Seq[In] => Out) extends GraphStage[FlowShape[In, Out]]
  {
    lazy val in    = shape.in
    lazy val out   = shape.out
    lazy val shape = FlowShape.of(Inlet[In](""), Outlet[Out](""))

    var count       = 0
    var finish      = false
    var windowQueue = Seq.empty[In]

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      setHandlers(
        in, out, new InHandler with OutHandler
        {
          @throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            count += 1
            windowQueue = windowQueue :+ grab(in)
            if (count == size) {
              count -= 1
              push(out, map.apply(windowQueue))
              windowQueue = windowQueue.tail
            }
            tryPull(in)
          }

          @throws[Exception](classOf[Exception])
          override def onPull(): Unit = finish match {
            case true if windowQueue.nonEmpty => push(out, map.apply(windowQueue)); windowQueue = windowQueue.tail
            case true => completeStage()
            case false => tryPull(in)
          }

          @throws[Exception](classOf[Exception])
          override def onUpstreamFinish(): Unit = { finish = true; onPull() }
        }
      )
    }
  }

}
