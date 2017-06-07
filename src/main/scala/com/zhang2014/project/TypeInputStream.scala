package com.zhang2014.project

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
