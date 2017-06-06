package com.zhang2014.project;


import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

//copy avro ByteBufferInputStream

public class ByteBufferInputStream extends InputStream
{
  private List<ByteBuffer> buffers;
  private int current;

  public ByteBufferInputStream(List<ByteBuffer> buffers)
  {
    this.buffers = buffers;
  }

  /**
   * @throws EOFException if EOF is reached.
   * @see InputStream#read()
   */
  @Override
  public int read() throws IOException
  {
    return getBuffer().get() & 0xff;
  }

  /**
   * @throws EOFException if EOF is reached before reading all the bytes.
   * @see InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    if (len == 0) {
      return 0;
    }
    ByteBuffer buffer = getBuffer();
    int remaining = buffer.remaining();
    if (len > remaining) {
      buffer.get(b, off, remaining);
      return remaining;
    } else {
      buffer.get(b, off, len);
      return len;
    }
  }

  /**
   * Read a buffer from the input without copying, if possible.
   *
   * @throws EOFException if EOF is reached before reading all the bytes.
   */
  public ByteBuffer readBuffer(int length) throws IOException
  {
    if (length == 0) {
      return ByteBuffer.allocate(0);
    }
    ByteBuffer buffer = getBuffer();
    if (buffer.remaining() == length) {           // can return current as-is?
      current++;
      return buffer;                              // return w/o copying
    }
    // punt: allocate a new buffer & copy into it
    ByteBuffer result = ByteBuffer.allocate(length);
    int start = 0;
    while (start < length) {
      start += read(result.array(), start, length - start);
    }
    return result;
  }

  /**
   * Returns the next non-empty buffer.
   *
   * @throws EOFException if EOF is reached before reading all the bytes.
   */
  private ByteBuffer getBuffer() throws IOException
  {
    while (current < buffers.size()) {
      ByteBuffer buffer = buffers.get(current);
      if (buffer.hasRemaining()) {
        return buffer;
      }
      current++;
    }
    throw new EOFException();
  }
}
