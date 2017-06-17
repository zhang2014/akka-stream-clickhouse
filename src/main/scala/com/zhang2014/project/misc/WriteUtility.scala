package com.zhang2014.project.misc

import java.nio.ByteBuffer

object WriteUtility
{
  def writeVarInt(buffer: ByteBuffer, value: Long): ByteBuffer = {
    val buff = maybeResize(buffer, 8)
    var varInt = value
    for (i <- 0 until 9) {
      val byt = varInt & 0x7F
      varInt match {
        case x if x > 0x7f => buff.put((byt | 0x80).toByte)
        case x => buff.put(byt.toByte)
      }
      varInt = varInt >> 7
      if (varInt == 0) {
        return buff
      }
    }
    buff
  }

  def writeUTF(buffer: ByteBuffer, value: String): ByteBuffer = {
    val bytes = value.getBytes("UTF-8")
    val buff = maybeResize(buffer, 8 + bytes.length)
    writeVarInt(buff, value.length)
    buff.put(bytes)
  }

  def maybeResize(buffer: ByteBuffer, needCapacity: Int): ByteBuffer = buffer match {
    case bf if bf.remaining() > needCapacity => buffer
    case bf if (bf.capacity() - bf.position()) > needCapacity => buffer.limit(bf.position() + needCapacity); buffer
    case bf =>
      val resizedBuffer = ByteBuffer.allocate(bf.position() + needCapacity)
      buffer.flip()
      while (buffer.hasRemaining) {
        resizedBuffer.put(buffer.get())
      }
      resizedBuffer
  }
}
