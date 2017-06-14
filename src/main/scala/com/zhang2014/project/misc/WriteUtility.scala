package com.zhang2014.project.misc

import java.nio.ByteBuffer

object WriteUtility
{
  def writeVarInt(buff: ByteBuffer, value: Long): Unit = {
    var varInt = value
    for (i <- 0 until 9) {
      val byt = varInt & 0x7F
      varInt match {
        case x if x > 0x7f => buff.put((byt | 0x80).toByte)
        case x => buff.put(byt.toByte)
      }
      varInt = varInt >> 7
      if (varInt == 0) {
        return
      }
    }
  }

  def writeUTF(buff: ByteBuffer, value: String): Unit = {
    writeVarInt(buff, value.length)
    buff.put(value.getBytes("UTF-8"))
  }
}
