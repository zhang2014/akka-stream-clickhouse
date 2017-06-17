package akka.ext

import java.nio.ByteBuffer

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

object CompressionBuffer
{

  case class CompressionBuffer(min: Int, buffer: ByteBuffer) extends Attribute

  def apply(min: Int, max: Int): Attributes = Attributes(new CompressionBuffer(min, ByteBuffer.allocate(max)))
}
