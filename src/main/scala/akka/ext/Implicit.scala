package akka.ext

import akka.stream.scaladsl.Source

import scala.concurrent.Future

object Implicit
{
  implicit def implicitA[T](seq: Seq[T]): ImplicitSeq[T] = ImplicitSeq[T](seq)

  implicit def implicitB[T](future: Future[T]): ImplicitFuture[T] = ImplicitFuture(future)

  implicit def implicitC[T, MAT](source: Source[T, MAT]): ImplicitSource[T, MAT] = ImplicitSource(source)
}
