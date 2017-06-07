package com.zhang2014.project

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

case class MarkReader(channel: FileChannel)
{
  var pos    = 0L
  val length = channel.size()
  val buffer = ByteBuffer.allocate(16)

  def readWith(runnable: (Int, Long, Long) => (Boolean, ByteBuffer)): List[ByteBuffer] = {
    var i = 0
    var maybeNeedRead = true
    var result = List.empty[ByteBuffer]
    while (maybeNeedRead && pos < length) {
      fillBuffer()
      val (needReadNext, data) = runnable(i, buffer.getLong(), buffer.getLong())

      i += 1
      result = data :: result
      maybeNeedRead = needReadNext
      if (maybeNeedRead) {
        pos = channel.position()
      }
    }
    result
  }

  def fillBuffer() = {
    buffer.clear()
    channel.read(buffer)
    buffer.flip()
  }
}
