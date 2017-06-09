package com.zhang2014.project.misc

import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}

import net.jpountz.lz4.LZ4Factory

trait CompressedReader
{
  def read(channel: FileChannel): ByteBuffer
}

object CompressedFactory
{
  def get(method: Byte): CompressedReader = toUInt8(method) match {
    case 0x82 => LZ4CompressedReader(1048576)
  }

  private final case class LZ4CompressedReader(blockMaxCompressedSize: Int) extends CompressedReader
  {
    private val headerBuffer     = ByteBuffer.allocate(8)
    private val compressedBuffer = ByteBuffer.allocate(blockMaxCompressedSize)

    headerBuffer.order(ByteOrder.LITTLE_ENDIAN)
    compressedBuffer.order(ByteOrder.LITTLE_ENDIAN)

    override def read(channel: FileChannel): ByteBuffer = {
      headerBuffer.clear()
      channel.read(headerBuffer)
      headerBuffer.flip()
      val (compressedSize, decompressedSize) = toUInt32(headerBuffer.getInt()) -> toUInt32(headerBuffer.getInt())
      compressedBuffer.limit(compressedSize.toInt)
      compressedBuffer.rewind()
      channel.read(compressedBuffer)
      compressedBuffer.rewind()

      val data = new Array[Byte](decompressedSize.toInt)
      val decompression = LZ4Factory.safeInstance.fastDecompressor
      val decompressedBuffer = ByteBuffer.wrap(data)

      decompression.decompress(
        compressedBuffer,
        compressedBuffer.position,
        decompressedBuffer,
        decompressedBuffer.position,
        decompressedBuffer.limit
      )
      decompressedBuffer
    }

    private def toUInt32(value: Int): Long = value & 0x0FFFFFFFFL
  }

  private def toUInt8(value: Byte): Int = value & 0x0FF

}
