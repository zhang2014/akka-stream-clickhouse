package com.zhang2014.project

import java.io.DataInputStream

import scala.util.Try

trait TypeInputStream[T]
{
  def next: Option[T]
}

case class ByteInputStream(input: DataInputStream) extends TypeInputStream[Byte]
{
  override def next: Option[Byte] = Try(input.readByte()).map(e => Some(e)).getOrElse(None)
}

case class IntInputStream(input: DataInputStream) extends TypeInputStream[Int]
{
  override def next: Option[Int] = Try(input.readInt()).map(e => Some(e)).getOrElse(None)
}

case class FloatInputSteam(input: DataInputStream) extends TypeInputStream[Float]
{
  override def next: Option[Float] = Try(input.readFloat()).map(e => Some(e)).getOrElse(None)
}

case class LongInputSteam(input: DataInputStream) extends TypeInputStream[Long]
{
  override def next: Option[Long] = Try(input.readLong()).map(e => Some(e)).getOrElse(None)
}
