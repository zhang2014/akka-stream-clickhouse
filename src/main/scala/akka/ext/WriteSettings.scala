package akka.ext

import java.nio.ByteBuffer

import akka.stream.Attributes
import akka.stream.Attributes.Attribute

object WriteSettings
{

  case class WriteSettings(
    minInBytes: Int = 65535,
    maxInBytes: Int = 1048576,
    var totalByteSize: Long = 0,
    blockSize: Int = 8192
  ) extends Attribute
  {
    val uncompressedBytes  = Array.ofDim[Byte](maxInBytes)
    val uncompressedBuffer = ByteBuffer.wrap(uncompressedBytes)
  }

  def apply(minInBytes: Int = 65535, maxInBytes: Int = 1048576, blockSize: Int = 8192): Attributes =
    Attributes(new WriteSettings(minInBytes, maxInBytes, 0, blockSize))
}
