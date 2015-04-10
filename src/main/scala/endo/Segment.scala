package endo

import java.io.{File, RandomAccessFile}
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.nio.channels.FileChannel
import java.util.ConcurrentModificationException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import sun.nio.ch.DirectBuffer

import scala.annotation.tailrec

object Segment {
  case class SegmentCorruptionException(msg: String) extends RuntimeException(msg)

  case class SegmentStats(fileName: String, currPosition: Int, numEntries: Long)

  // ensure only one create/delete segment operation against disk
  object DirectoryLock

  private val logger = Logger(LoggerFactory.getLogger(Segment.getClass))

  def apply(directory: String, size: Long, segmentNumber: Integer, queue: EntryQueue): Segment = {
    DirectoryLock.synchronized {
      val fileName = s"${directory}/_${segmentNumber}"
      val f = new File(fileName)
      f.createNewFile()
      new Segment(f, size, queue)
    }
  }
}

protected class Segment(file: File, size: Long, queue: EntryQueue) {
  import Segment._

  private val mmap: MappedByteBuffer = createMmapping()
  private val currPos = new AtomicInteger(0) // not really necessary but for sanity to keep track of position in mmap
  private val rwLock = new ReentrantReadWriteLock() // protects reads when unmap occurs, should not read an unmapped buffer!!!
  private val numEntries = new AtomicLong(0)

  def append(payload: Payload, fsync: Boolean): Option[Record] = {
    this.synchronized {
      val pos = currPos.get
      val buffer = mmap.duplicate().position(pos).asInstanceOf[ByteBuffer]
      val size = payload.payloadLength + Payload.metadataSize

      if (buffer.remaining() > size) {
        buffer.put(payload.buffer)

        numEntries.incrementAndGet()

        payload.buffer.rewind // reset after use
        if (!currPos.compareAndSet(pos, pos + size))
          throw new ConcurrentModificationException("Segment position concurrently modified during append")

        if (fsync) flush()
        Some(Record(this, queue, pos, size))
      } else {
        logger.debug(s"No more buffer space left for segment file: ${file.getName}")
        None
      }
    }
  }

  @tailrec
  final def initRecords(offset: Int = 0, acc: List[Record] = Nil): List[Record] = {
    val record = readRecord(offset)
    record match {
      case Some(r) =>
        val nextPos = r.offset + r.length
        currPos.set(nextPos)
        numEntries.incrementAndGet()
        initRecords(nextPos, r :: acc)
      case None =>
        acc
    }
  }

  @tailrec
  final def getString(offset: Int = 0, acc: List[String] = Nil): String = {
    val record = readRecord(offset)
    record match {
      case Some(r) =>
        val nextPos = r.offset + r.length
        getString(nextPos, r.toString :: acc)
      case None =>
        acc.reverse.mkString("\n")
    }
  }

  def read(offset: Int, length: Int): Payload = {
    withReadLock(rwLock) {
      val record = new Payload(slice(mmap.duplicate(), offset, length))
      if (!record.validate) throw SegmentCorruptionException("Payload failed checksum validation")
      record
    }
  }

  private def readRecord(offset: Int): Option[Record] = {
    val payload = mmap.duplicate().position(offset).asInstanceOf[ByteBuffer]

    val activeFlag = payload.get()

    if (payload.remaining() <= 0) {
      logger.debug("Properly reached end of segment")
      None
    } else if (payload.remaining() < Payload.metadataSize + 2) {
      logger.error(s"Unexpected data found at end of segment: ${file.getName}!")
      None
    } else if (activeFlag != 1) {
      logger.debug("Found inactive record within mmap, during init.")
      None
    } else {
      val statusFlag = payload.get()
      val checkSum = payload.getLong // TODO: need to use it to verify
      val length = payload.getInt
      Some(Record(this, queue, offset, length + Payload.metadataSize))
    }
  }


  def flush(): Unit = {
    this.synchronized {
      mmap.force()
    }
  }

  def delete(): Unit = {
    DirectoryLock.synchronized {
      unmap()
      file.delete()
    }
  }

  // cue kenny loggins 'danger zone'
  def unmap(): Unit = {
    withWriteLock(rwLock) {
      mmap.asInstanceOf[DirectBuffer].cleaner().clean()
    }
  }

  def stats: SegmentStats = SegmentStats(file.getName, currPos.get(), numEntries.get())

  def getPosition: Int = currPos.get()
  def getNumEntries: Long = numEntries.get()

  private def createMmapping(): MappedByteBuffer = {
    val raf = new RandomAccessFile(file, "rw")
    raf.setLength(size)

    var buf: MappedByteBuffer = null
    try {
      buf = raf.getChannel.map(FileChannel.MapMode.READ_WRITE, 0, size)
    } finally {
      raf.close()
    }
    buf
  }

}
