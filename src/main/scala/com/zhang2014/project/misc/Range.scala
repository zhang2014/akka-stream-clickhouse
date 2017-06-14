package com.zhang2014.project.misc

trait Range
{
  def begin: Long

  def end: Long
}

case class UnCompressedRange(begin: Long, end: Long) extends Range

case class CompressedRange(begin: Long, end: Long, unCompressedBegin: Long, unCompressedEnd: Long) extends Range
