package com.zhang2014.project.misc

import java.io.{EOFException, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import scala.util.{Failure, Success}

case class ByteBufferSourceInputStream(source: Source[ByteBuffer, Any])
  (implicit system: ActorSystem, materializer: Materializer) extends InputStream
{

  var buff  = Option.empty[ByteBuffer]
  val queue = source.runWith(Sink.queue())
  implicit val dispatcher = system.dispatcher

  override def read: Int = getBuffer.get & 0xff

  override def read(b: Array[Byte]): Int = read(b, 0, b.length)

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val buffer = getBuffer
    (len, buffer.remaining()) match {
      case (0, _) => 0
      case (length, remain) if length > remain => buffer.get(b, off, remain); remain
      case (length, remain) => buffer.get(b, off, len); len
    }
  }

  private def getBuffer = {
    val b = buff match {
      case Some(buffer) if buffer.hasRemaining => buffer
      case _ =>
        val latch = new CountDownLatch(1)
        val future = queue.pull()
        future.onComplete { case _ => latch.countDown() }
        latch.await()
        future.value.get match {
          case Failure(ex) => throw ex
          case Success(None) => throw new EOFException
          case Success(Some(buffer)) => buffer
        }
    }
    buff = Some(b)
    b
  }
}
