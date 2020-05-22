package com.dist.consensus

import java.nio.ByteBuffer
import java.nio.channels.FileChannel

class WalEntryDeserializer(logChannel: FileChannel) {
  val intBuffer = WriteAheadLog.newBuffer(WriteAheadLog.sizeOfInt)
  val longBuffer = WriteAheadLog.newBuffer(WriteAheadLog.sizeOfLong)

  def readEntry() = {
    val entrySize: Int = readInt
    val entryType: Int = readInt
    val entryId: Long = readLong
    val (walEntryData, position) = readData(entrySize)

    (WalEntry(entryId, walEntryData.array()), entrySize + WriteAheadLog.sizeOfInt, position)
  }


  private def readData(entrySize: Int) = {
    val dataSize = entrySize - (WriteAheadLog.sizeOfInt + WriteAheadLog.sizeOfLong)
    val (walEntryData, position) = readFromChannel(WriteAheadLog.newBuffer(dataSize))
    (walEntryData, position)
  }

  private def readLong = {
    val (entryIdBuffer, position) = readFromChannel(longBuffer)
    val entryId = entryIdBuffer.getLong()
    entryId
  }

  private def readInt = {
    val (entrySizeBuffer, position) = readFromChannel(intBuffer)
    val entrySize = entrySizeBuffer.getInt()
    entrySize
  }

  private def readFromChannel(buffer:ByteBuffer):(ByteBuffer, Long) = {
    buffer.clear()
    val filePosition = readFromChannel(logChannel, buffer)
    (buffer.flip(), filePosition)
  }

  private def readFromChannel(channel:FileChannel, buffer:ByteBuffer) = {
    while (logChannel.read(buffer) > 0) {}
    logChannel.position()
  }
}

case class WalEntry(entryId:Long, data:Array[Byte], entryType:Int = 0) {

  def serialize():ByteBuffer = {
    val bufferSize = entrySize + 4 //4 bytes for record length + walEntry size
    val buffer = WriteAheadLog.newBuffer(bufferSize)
    buffer.clear()
    buffer.putInt(entrySize)
    buffer.putInt(0) //normal entry
    buffer.putLong(entryId)
    buffer.put(data)
  }

  def entrySize = {
    data.length + WriteAheadLog.sizeOfLong + WriteAheadLog.sizeOfInt //size of all the fields
  }
}



