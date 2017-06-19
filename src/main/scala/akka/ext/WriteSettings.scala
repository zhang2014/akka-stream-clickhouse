package akka.ext

import java.nio.ByteBuffer

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

object WriteSettings
{

  case class WriteSettings(min: Int, max: Int, var totalSize: Long) extends Attribute
  {
    val bytes  = Array.ofDim[Byte](max)
    val buffer = ByteBuffer.wrap(bytes)
  }

  def apply(min: Int, max: Int): Attributes = Attributes(new WriteSettings(min, max, 0))
}
