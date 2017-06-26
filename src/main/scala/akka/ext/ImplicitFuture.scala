package akka.ext

import java.util.concurrent.CountDownLatch

import scala.concurrent.{ExecutionContext, Future}

case class ImplicitFuture[T](future: Future[T])
{
  def result()(implicit ec: ExecutionContext): T = {
    val latch = new CountDownLatch(1)
    future.onComplete(_ => latch.countDown())
    latch.await()
    future.value.get.get
  }
}
