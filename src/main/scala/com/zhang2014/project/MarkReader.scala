package com.zhang2014.project

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

case class MarkReader(channel: FileChannel)
{
  var pos    = 0L
  val buffer = ByteBuffer.allocate(16)

  def readWith(runnable: (Int, Long, Long) => (Boolean, ByteBuffer)): List[ByteBuffer] = {
    if (pos == channel.size) {
      Nil
    } else {
      channel.position(pos)
      var i = 0
      var isEnd = false
      var res = List.empty[ByteBuffer]
      while (!isEnd) {
        buffer.clear()
        channel.read(buffer)
        buffer.flip()
        val (end, byteBuffer) = runnable(i, buffer.getLong(), buffer.getLong())
        res = byteBuffer :: res
        isEnd = end
        i += 1
      }
      pos = channel.position() - buffer.capacity()
      res
    }
  }
}
