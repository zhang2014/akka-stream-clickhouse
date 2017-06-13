package com.zhang2014.project.misc

import java.io.UTFDataFormatException
import java.nio.{ByteBuffer, LongBuffer}

import com.google.common.io.LittleEndianDataInputStream

object ReadUtility
{

  def readVarInt(input: LittleEndianDataInputStream): Long = {
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

  def readUTF(length: Int, input: LittleEndianDataInputStream) = {
    val chars = Array.ofDim[Char](length).map { _ =>
      val char = input.readByte() & 0xff
      char >> 4 match {
        case x if x < 7 => char.toChar
        case 12 | 13 => readDoubleUFT(char.toChar, input)
        case 14 => readThreeUFT(char.toChar, input)
      }
    }
    new String(chars)
  }

  def int2UInt(byte: Byte): Int = byte & 0x0ff

  def int2UInt(short: Short): Int = short & 0x0ffff

  def int2UInt(int: Int): Long = int & 0x0ffffffffL

  def int2UInt(long: Long): BigInt = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(long)
    BigInt(1, buffer.array())
  }

  def list2Product(list: List[Any]): Product = list match {
    case e1 :: Nil => Tuple1(e1)
    case e1 :: e2 :: Nil => Tuple2(e1, e2)
    case e1 :: e2 :: e3 :: Nil => Tuple3(e1, e2, e3)
    case e1 :: e2 :: e3 :: e4 :: Nil => Tuple4(e1, e2, e3, e4)
    case e1 :: e2 :: e3 :: e4 :: e5 :: Nil => Tuple5(e1, e2, e3, e4, e5)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: Nil => Tuple6(e1, e2, e3, e4, e5, e6)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: Nil => Tuple7(e1, e2, e3, e4, e5, e6, e7)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: Nil => Tuple8(e1, e2, e3, e4, e5, e6, e7, e8)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: Nil => Tuple9(e1, e2, e3, e4, e5, e6, e7, e8, e9)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: Nil =>
      Tuple10(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: Nil =>
      Tuple11(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: Nil =>
      Tuple12(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: Nil =>
      Tuple13(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: Nil =>
      Tuple14(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: Nil =>
      Tuple15(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: Nil =>
      Tuple16(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16)
    case e1 ::
      e2 :: e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: Nil =>
      Tuple17(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17)
    case e1 :: e2 ::
      e3 :: e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: e18 :: Nil =>
      Tuple18(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18)
    case e1 :: e2 :: e3 ::
      e4 :: e5 :: e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: e18 :: e19 :: Nil =>
      Tuple19(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19)
    case e1 :: e2 :: e3 :: e4 :: e5 ::
      e6 :: e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: e18 :: e19 :: e20 :: Nil =>
      Tuple20(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 ::
      e7 :: e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: e18 :: e19 :: e20 :: e21 :: Nil =>
      Tuple21(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21)
    case e1 :: e2 :: e3 :: e4 :: e5 :: e6 :: e7 ::
      e8 :: e9 :: e10 :: e11 :: e12 :: e13 :: e14 :: e15 :: e16 :: e17 :: e18 :: e19 :: e20 :: e21 :: e22 :: Nil =>
      Tuple22(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22)
  }

  private def readDoubleUFT(char1: Char, input: LittleEndianDataInputStream) = {
    val char2 = input.readByte().toInt
    if ((char2 & 0xC0) == 0x80) {
      ((char1 & 0x1F) << 6 | (char2 & 0x3F)).toChar
    } else {
      throw new UTFDataFormatException("malformed input around byte " + char2)
    }
  }

  private def readThreeUFT(char1: Char, input: LittleEndianDataInputStream) = {
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


}
