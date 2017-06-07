package com.zhang2014.project

import java.io.{EOFException, UTFDataFormatException}

import com.google.common.io.LittleEndianDataInputStream

import scala.util.Try

trait TypeInputStream[T]
{
  def next: Option[T]
}

case class ByteInputStream(input: LittleEndianDataInputStream) extends TypeInputStream[Byte]
{
  override def next: Option[Byte] = Try(input.readByte()).map(e => Some(e)).getOrElse(None)
}

case class IntInputStream(input: LittleEndianDataInputStream) extends TypeInputStream[Int]
{
  override def next: Option[Int] = Try(input.readInt()).map(e => Some(e)).getOrElse(None)
}

case class FloatInputSteam(input: LittleEndianDataInputStream) extends TypeInputStream[Float]
{
  override def next: Option[Float] = Try(input.readFloat()).map(e => Some(e)).getOrElse(None)
}

case class LongInputSteam(input: LittleEndianDataInputStream) extends TypeInputStream[Long]
{
  override def next: Option[Long] = Try(input.readLong()).map(e => Some(e)).getOrElse(None)
}

case class StringInputStream(input: LittleEndianDataInputStream) extends TypeInputStream[String]
{
  override def next: Option[String] = {
    Try(readUTF()).map(e => Some(e)).recover { case _: EOFException => None }.get
  }

  def readUTF() = {
    val length = getUTFLength.toInt
    val chars = Array.ofDim[Char](length).map { _ =>
      val char = input.readByte() & 0xff
      char >> 4 match {
        case x if x < 7 => char.toChar
        case 12 | 13 => readDoubleUFT(char.toChar)
        case 14 => readThreeUFT(char.toChar)
      }
    }
    new String(chars)
  }

  private def readDoubleUFT(char1: Char) = {
    val char2 = input.readByte().toInt
    if ((char2 & 0xC0) == 0x80) {
      ((char1 & 0x1F) << 6 | (char2 & 0x3F)).toChar
    } else {
      throw new UTFDataFormatException("malformed input around byte " + char2)
    }
  }

  private def readThreeUFT(char1: Char) = {
    val char2 = input.readByte().toInt
    val char3 = input.readByte().toInt
    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
      throw new UTFDataFormatException(
        "malformed input around byte " + char2
      )
    } else {
      (((char1 & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0)).toChar
    }
  }

  private def getUTFLength: Long = {
    var length = 0
    for (i <- 0 until 9) {
      val byt = input.readByte()
      length = length | ((byt & 0x7F) << (7 * i))
      if ((byt & 0x80) == 0) {
        return length
      }
    }
    length
  }
}
