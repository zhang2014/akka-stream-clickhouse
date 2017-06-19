package com.zhang2014.project.sink

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util

import akka.actor.ActorSystem
import akka.ext.WriteSettings
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSource
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.zhang2014.project.misc.{CompressedFactory, CompressedRange, UnCompressedRange}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.Await
import scala.concurrent.duration._

class FileSinkTest extends WordSpec with Matchers
{
  implicit val system       = ActorSystem()
  lazy     val settings     = ActorMaterializerSettings(system).withInputBuffer(initialSize = 1, maxSize = 1)
  implicit val materializer = ActorMaterializer(settings)

  "FileSink" should {

    "new data file with uncompressed file" in {
      // | block 1(0...2) | block 2(3...5) | block 3(6)
      val data = ByteBuffer.wrap(intArray2Bytes(7))
      val dataFile = createTempFileAndChannel()
      val (publisher, completeHandle) = TestSource.probe[ByteBuffer]
        .toMat(FileSink(dataFile.getAbsolutePath, UnCompressedRange(0, -1)))(Keep.both).run()

      publisher.sendNext(splitByteBuffer(data, 0, 12))
      publisher.sendNext(splitByteBuffer(data, 12, 24))
      publisher.sendNext(splitByteBuffer(data, 24, 28))
      publisher.sendComplete()

      Await.result(completeHandle, 3.seconds)
      loadUncompressedDataFile(dataFile.getAbsolutePath, 28).array() should ===(data.array())
    }

    "overwrite data file with uncompressed file" in {
      // | block 1(0...2) | block 2(3...5) | block 3(6)
      val dataFile = createTempFileAndChannel()
      new RandomAccessFile(dataFile, "rw").getChannel.write(ByteBuffer.wrap(intArray2Bytes(7)))

      val (publisher, completeHandle) = TestSource.probe[ByteBuffer]
        .toMat(FileSink(dataFile.getAbsolutePath, UnCompressedRange(12, 24)))(Keep.both).run()

      publisher.sendNext(ByteBuffer.wrap(intArray2Bytes(3)))
      publisher.sendNext(ByteBuffer.wrap(intArray2Bytes(3)))
      publisher.sendComplete()

      Await.result(completeHandle, 3.seconds)
      loadUncompressedDataFile(dataFile.getAbsolutePath, 40).array() should ===(
        Array(0, 1, 2, 0, 1, 2, 0, 1, 2, 6)
          .flatMap(int => Array(int.toByte, (int >> 8).toByte, (int >> 16).toByte, (int >> 24).toByte))
      )
    }

    "new data file with compressed file" in {
      // | block1 = (0...3 + 4[1]) | block2 = (4[2,3,4] + 5...7 + 8[1,2])|block3 = (8[3,4] + 9...11 + 12[1,2,3])|block4 = (12[4] + 13...16)
      val data = ByteBuffer.wrap(intArray2Bytes(17))
      val dataFile = createTempFileAndChannel()
      val (publisher, completeHandle) = TestSource.probe[ByteBuffer]
        .toMat(FileSink(dataFile.getAbsolutePath, CompressedRange(0, -1, 0, -1)))(Keep.both)
        .withAttributes(WriteSettings(7, 7)).run()

      publisher.sendNext(data)
      publisher.sendComplete()

      Await.result(completeHandle, 3.seconds)
      val actualData = loadCompressedDataFile(dataFile.getAbsolutePath)
      util.Arrays.copyOf(actualData.array(), actualData.limit()) should ===(data.array())
    }
  }

  private def createTempFileAndChannel() = File.createTempFile("test_", "_write_file")

  private def intArray2Bytes(max: Int): Array[Byte] = (0 until max)
    .flatMap(int => Array(int.toByte, (int >> 8).toByte, (int >> 16).toByte, (int >> 24).toByte)).toArray

  private def splitByteBuffer(buffer: ByteBuffer, offset: Int, limit: Int) = {
    val bf = (offset until limit)
      .foldLeft(ByteBuffer.allocate(limit - offset))((l, r) => l.put(buffer.get(r)))
    bf.flip()
    bf
  }

  private def loadCompressedDataFile(dataFile: String) = new RandomAccessFile(dataFile, "r").getChannel match {
    case channel =>
      val buffer = ByteBuffer.allocate(channel.size().toInt)
      while (channel.position() < channel.size()) {
        val head = ByteBuffer.allocate(17)
        channel.read(head)
        val readBuffer = CompressedFactory.get(head.get(16)).read(channel)
        while (readBuffer.hasRemaining) {
          buffer.put(readBuffer.get)
        }
      }
      buffer.flip()
      buffer
  }

  private def loadUncompressedDataFile(dataFile: String, expectFileSize: Long) = new RandomAccessFile(
    dataFile,
    "r"
  ) match {
    case file => file.getChannel match {
      case channel =>
        channel.size() should ===(expectFileSize)
        val buffer = ByteBuffer.allocate(expectFileSize.toInt)
        channel.read(buffer)
        buffer
    }
  }
}
