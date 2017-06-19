package com.zhang2014.project.misc

import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}

import net.jpountz.lz4.LZ4Factory

trait CompressionEngine
{
  def read(channel: FileChannel): ByteBuffer

  def compression(data: ByteBuffer): ByteBuffer
}

object CompressedFactory
{
  def get(method: Byte): CompressionEngine = toUInt8(method) match {
    case 0x82 => LZ4CompressionEngine(1048576)
  }

  private def toUInt8(value: Byte): Int = value & 0x0FF

  private final case class LZ4CompressionEngine(blockMaxCompressedSize: Int) extends CompressionEngine
  {
    private val headBuffer  = ByteBuffer.allocate(8)
    private var readBuffer  = ByteBuffer.allocate(blockMaxCompressedSize)
    private var writeBuffer = ByteBuffer.allocate(blockMaxCompressedSize)

    private val compressor   = LZ4Factory.safeInstance.fastCompressor
    private val decompressor = LZ4Factory.safeInstance.fastDecompressor

    headBuffer.order(ByteOrder.LITTLE_ENDIAN)
    readBuffer.order(ByteOrder.LITTLE_ENDIAN)
    writeBuffer.order(ByteOrder.LITTLE_ENDIAN)

    override def read(channel: FileChannel): ByteBuffer = {
      val (compressedSize, decompressedSize) = readCompressionHead(channel)
      readBuffer = getCreateReadBuffer(compressedSize)
      readBuffer.limit(compressedSize.toInt)
      readBuffer.rewind()
      channel.read(readBuffer)
      readBuffer.flip()

      val data = new Array[Byte](decompressedSize.toInt)

      val decompressedBuffer = ByteBuffer.wrap(data)

      decompressor.decompress(
        readBuffer,
        readBuffer.position,
        decompressedBuffer,
        decompressedBuffer.position,
        decompressedBuffer.limit
      )
      decompressedBuffer
    }

    override def compression(data: ByteBuffer): ByteBuffer = {
      writeBuffer.position(writeBuffer.position() + 8)
      compressor.compress(data, writeBuffer)
      writeBuffer.flip()
      writeBuffer.putInt(writeBuffer.limit() - 8)
      writeBuffer.putInt(data.limit())
      writeBuffer.rewind()
      writeBuffer
    }

    private def readCompressionHead(channel: FileChannel) = {
      headBuffer.clear()
      channel.read(headBuffer)
      headBuffer.flip()
      toUInt32(headBuffer.getInt()) -> toUInt32(headBuffer.getInt())
    }

    private def toUInt32(value: Int): Long = value & 0x0FFFFFFFFL

    private def getCreateReadBuffer(needCapacity: Long) = {
      readBuffer match {
        case buffer if buffer.capacity() < needCapacity => ByteBuffer.allocate(needCapacity.toInt)
        case buffer => buffer.clear; buffer
      }
    }

    private def getCreateWriteBuffer(needCapacity: Long) = {
      writeBuffer match {
        case buffer if buffer.capacity() < needCapacity => ByteBuffer.allocate(needCapacity.toInt)
        case buffer => buffer.clear(); buffer
      }
    }
  }
}
