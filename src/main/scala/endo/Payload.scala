package endo

import java.nio.ByteBuffer
import java.util.zip.CRC32

import endo.Payload.{PayloadCorruptionException, Status}

object Payload {
  val metadataSize = 15

  case class PayloadCorruptionException(msg: String) extends RuntimeException

  object Status extends Enumeration {
    type Status = Value
    val Completed, Unclaimed, Claimed = Value
  }

  def apply(payload: Array[Byte]): Payload = {
    new Payload(payload, true, false, Status.Unclaimed)
  }

  def apply(payload: ByteBuffer): Payload = {
    new Payload(payload, true, false, Status.Unclaimed)
  }
}

/**
 * Input object representation which is 'offered' to the queue, containing the binary blob.
 */
class Payload(val buffer: ByteBuffer) {
  import Payload.Status._

  /**
   * ByteBuffer interface
   */
  def this(payload: ByteBuffer, activeFlag: Boolean, nextFlag: Boolean, status: Status.Value) {
    this(ByteBuffer.allocate(Payload.metadataSize + payload.limit))
    buffer.position(0)
    buffer.put(boolToByte(activeFlag))
    buffer.put(status.id.toByte)
    buffer.putLong(calcCheckSum(payload, payload.limit))
    buffer.putInt(payload.limit)

    payload.mark()
    buffer.put(payload)
    payload.reset()

    buffer.put(boolToByte(nextFlag))
    buffer.rewind()
  }

  /**
   * Byte array interface
   */
  def this(payload: Array[Byte], activeFlag: Boolean, nextFlag: Boolean, status: Status.Value) {
    this(ByteBuffer.allocate(Payload.metadataSize + payload.length))
    buffer.position(0)
    buffer.put(boolToByte(activeFlag))
    buffer.put(status.id.toByte)
    buffer.putLong(calcCheckSum(ByteBuffer.wrap(payload), payload.length))
    buffer.putInt(payload.length)
    buffer.put(payload)
    buffer.put(boolToByte(nextFlag))
    buffer.rewind()
  }

  def setStatus(status: Status): Unit = buffer.duplicate().put(1, status.id.toByte)

  // accessors
  def activeFlag: Boolean = byteToFlag(buffer.asReadOnlyBuffer().get(0))
  def status: Status = Status(buffer.asReadOnlyBuffer.get(1))
  def checkSum: Long = buffer.asReadOnlyBuffer.getLong(2)
  def payloadLength: Int = buffer.asReadOnlyBuffer.getInt(10)
  def payload: ByteBuffer = slice(buffer.asReadOnlyBuffer(), Payload.metadataSize - 1, payloadLength)
  def nextFlag: Boolean = byteToFlag(buffer.asReadOnlyBuffer().get(Payload.metadataSize + payloadLength - 1))

  def validate: Boolean = {
    val c = checkSum
    val d = calcCheckSum(payload, payloadLength)
    c == d
  }

  override def toString: String = {
    s"""
       | active: $activeFlag
       | status: $status
       | payloadLength: $payloadLength
       | payload: $payload
       | nextFlag: $nextFlag
     """.stripMargin
  }

  private def boolToByte(flag: Boolean): Byte = if (flag) 1.toByte else 0.toByte

  private def byteToFlag(byte: Byte): Boolean = {
    if (byte.toInt == 1)  true
    else if (byte.toInt == 0) false
    else throw new PayloadCorruptionException(s"Incorrect flag byte value of: ${byte.toInt}")
  }

  private def calcCheckSum(payload: ByteBuffer, size: Int): Long = {

    val p = payload.duplicate()
    val crc = new CRC32()
    var i = 4
    while(i < size) {
      crc.update(p.get(i))
      i = i + 1
    }
    crc.getValue
  }

}
