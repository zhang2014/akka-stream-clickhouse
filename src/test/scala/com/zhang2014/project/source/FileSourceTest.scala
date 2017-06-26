package com.zhang2014.project.source

import java.io.{File, RandomAccessFile}
import java.nio.{ByteBuffer, ByteOrder}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import com.zhang2014.project.misc.{CompressedRange, CompressedFactory, UnCompressedRange}
import org.scalatest.{Matchers, WordSpec}

class FileSourceTest extends WordSpec with Matchers
{
  implicit val system       = ActorSystem()
  implicit val materializer = ActorMaterializer()

  "FileSource" should {

    "successfully read full data with uncompressed file" in {
      // | block 1(1...num) | block 2(num...num) | block 3(num)
      val maxValue = 8192 * 2 / 4 + 1
      val dataFile = createUnCompressedDataFile(maxValue)
      val sub = FileSource(dataFile.getAbsolutePath, UnCompressedRange(0, -1)).toMat(TestSink.probe)(Keep.right).run()

      var buffer = sub.requestNext()
      (0 until maxValue).foreach {
        case expectValue if buffer.hasRemaining => buffer.getInt() should ===(expectValue)
        case expectValue => buffer = sub.requestNext(); buffer.getInt() should ===(expectValue)
      }
      sub.request(1).expectComplete()
    }

    "successfully read block 2 with uncompressed file" in {
      // | block 1(1...num) | block 2(num...num) | block 3(num)
      val maxValue = 8192 * 2 / 4 + 1
      val dataFile = createUnCompressedDataFile(maxValue)
      val sub = FileSource(dataFile.getAbsolutePath, UnCompressedRange(8192, 8192 * 2))
        .toMat(TestSink.probe)(Keep.right).run()

      var buffer = sub.requestNext()
      (8192 / 4 until (8192 * 2 / 4)).foreach {
        case expectValue if buffer.hasRemaining => buffer.getInt() should ===(expectValue)
        case expectValue => buffer = sub.requestNext(); buffer.getInt() should ===(expectValue)
      }
      sub.request(1).expectComplete()
    }

    "successfully read full data with compressed file" in {
      // | block1 = (0...3 + 4[1]) | block2 = (4[2,3,4] + 5...7 + 8[1,2])|block3 = (8[3,4] + 9...11 + 12[1,2,3])|block4 = (12[4] + 13...16)
      val (dataFile, _) = createCompressedDataFile(17, 17)
      val sub = FileSource(dataFile.getAbsolutePath, CompressedRange(0, -1, 0, -1))
        .toMat(TestSink.probe)(Keep.right).run()

      var buffer = sub.requestNext()
      (0 until 17).flatMap(int => Array(int.toByte, (int >> 8).toByte, (int >> 16).toByte, (int >> 24).toByte))
        .foreach {
          case expectValue if buffer.hasRemaining => buffer.get() should ===(expectValue)
          case expectValue => buffer = sub.requestNext(); buffer.get() should ===(expectValue)
        }
      sub.request(1).expectComplete()
    }

    "successfully read block2 data with compressed file" in {
      // | block1 = (0...3 + 4[1]) | block2 = (4[2,3,4] + 5...7 + 8[1,2])|block3 = (8[3,4] + 9...11 + 12[1,2,3])|block4 = (12[4] + 13...16)
      val (dataFile, compressedOffsets) = createCompressedDataFile(17, 17)
      val sub = FileSource(dataFile.getAbsolutePath, CompressedRange(compressedOffsets(1), compressedOffsets(3), 3, 1))
        .toMat(TestSink.probe)(Keep.right).run()

      var buffer = sub.requestNext()
      (5 until 13).flatMap(int => Array(int.toByte, (int >> 8).toByte, (int >> 16).toByte, (int >> 24).toByte))
        .foreach {
          case expectValue if buffer.hasRemaining => buffer.get() should ===(expectValue)
          case expectValue => buffer = sub.requestNext(); buffer.get() should ===(expectValue)
        }
      sub.request(1).expectComplete()
    }
  }

  private def createUnCompressedDataFile(size: Int): File = {
    val tempFile = File.createTempFile("test_", "_data")
    val tempFileChannel = new RandomAccessFile(tempFile, "rw").getChannel
    val buffer = (0 until size)
      .foldLeft(ByteBuffer.allocate(4 * size).order(ByteOrder.LITTLE_ENDIAN))((l, r) => l.putInt(r))
    buffer.flip()
    tempFileChannel.write(buffer)
    tempFileChannel.force(true)
    tempFileChannel.close()
    tempFile.deleteOnExit()
    tempFile
  }

  private def createCompressedDataFile(size: Int, unCompressedBufferSize: Int): (File, Array[Long]) = {
    val tempFile = File.createTempFile("test_", "_data")
    val dataBuffer = ByteBuffer.allocate(unCompressedBufferSize)
    val tempFileChannel = new RandomAccessFile(tempFile, "rw").getChannel
    val compressionHead = ByteBuffer.allocate(17).putLong(0l).putLong(0L).put(0x82.toByte)

    var compressedOffset = List(0L)

    def flushData(): Unit = {
      if (dataBuffer.position() > 0) {
        compressionHead.flip()
        tempFileChannel.write(compressionHead)
        dataBuffer.flip()
        tempFileChannel.write(CompressedFactory.get(0x82.toByte).compression(dataBuffer))
        compressedOffset = compressedOffset :+ tempFileChannel.position()
        dataBuffer.clear()
      }
    }

    (0 until size).flatMap(int => Array(int.toByte, (int >> 8).toByte, (int >> 16).toByte, (int >> 24).toByte))
      .foreach {
        case byte if dataBuffer.hasRemaining => dataBuffer.put(byte)
        case byte => flushData(); dataBuffer.put(byte)
      }
    flushData()
    tempFileChannel.force(true)
    tempFileChannel.close()
    tempFile.deleteOnExit()
    tempFile -> compressedOffset.toArray
  }
}
