package endo

import java.io.File
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.typesafe.scalalogging.Logger
import endo.Record.RecordStats
import endo.Segment.SegmentStats
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

import scala.concurrent.duration.Duration

object EntryQueue {
  case class QueueCorruptionException(msg: String) extends RuntimeException(msg)
  private val logger = Logger(LoggerFactory.getLogger(EntryQueue.getClass))

  case class QueueStats(queueId: String,
                        numEnqueued: Long,
                        numCompleted: Long,
                        numRetried: Long,
                        segments: Seq[SegmentStats],
                        records: Seq[RecordStats])

  def apply(queueId: String, maxSize: Int, dirName: String, segmentSize: Int): EntryQueue = {
    val name = s"$dirName/$queueId"
    val d = new File(name)
    if (!d.exists()) d.mkdir()

    new EntryQueue(queueId, maxSize, name, segmentSize)
  }
}

protected class EntryQueue(val queueId: String, maxSize: Int, dirName: String, segmentSize: Int) {
  import EntryQueue._

  private val queue = new LinkedBlockingQueue[Record](maxSize)

  // append to last slab, read from first slab
  // TODO consider using ConcurrentLinkedQueue
  private val segments = new ConcurrentSkipListMap[Integer, Segment]()
  private val nextSegmentId = new AtomicInteger(0)

  // stats
  private val numEnqueued  = new AtomicLong(0)
  private val numCompleted = new AtomicLong(0)
  private val numRetried = new AtomicLong(0)

  def offer(entry: Payload, fsync: Boolean, timeout: Duration = Duration.Inf): Boolean = {
    this.synchronized {
      val segment = if (segments.isEmpty) {
        createSegment()
      } else segments.lastEntry().getValue

      val record = segment.append(entry, fsync) match {
        case Some(rec) => rec
        case None =>
          val newSegment = createSegment()

          // try again...
          val result = newSegment.append(entry, fsync)
          result match {
            case Some(rec) => rec
            case None => throw new RuntimeException(s"Unable to offer payload to $newSegment")
          }
      }

      // Duration.Inf will throw IllegalArgException when called with 'toMillis'
      val ms = if (timeout == Duration.Inf) Long.MaxValue else timeout.toMillis
      if (queue.offer(record, ms, TimeUnit.MILLISECONDS)) {
        numEnqueued.incrementAndGet()
        true
      } else {
        false
      }
    }
  }

  def poll(timeout: Duration = Duration.Inf): Option[Record] = {
    try {
      val ms = if (timeout == Duration.Inf) Long.MaxValue else timeout.toMillis
      val record = Option(queue.poll(ms, TimeUnit.MILLISECONDS))
      record.foreach(_.claimed())
      record
    } catch {
      case _: TimeoutException =>
        logger.info(s"Polling timed out after $timeout.toMillis milliseconds")
        None
    }
  }

  /**
   * Should only be called during initialization.
   */
  protected[endo] def loadFromDisk(): Unit = {
    this.synchronized {
      new File(dirName).listFiles().foreach { f =>
        """^_(\d+)$""".r.findFirstMatchIn(f.getName) match {
          case Some(m) => {
            val id = m.group(1).toInt
            val segment = Segment(dirName, segmentSize, id, this)
            if (segments.put(id, segment) != null)
              throw new QueueCorruptionException(s"Attempting to create a new segment with pre-existing id: $id")
          }
          case None =>
        }
      }

      if (!segments.isEmpty) {
        nextSegmentId.set(segments.lastEntry().getKey + 1)
      }

      // init queue with only non-completed tasks
      // TODO make sure in order
      val itr = segments.values().iterator()
      while(itr.hasNext){
        val segment = itr.next()
        val records = segment.initRecords()

        records.foreach { r =>
          if (!r.isCompleted) {
            r.unclaimed()
            queue.add(r)
            numEnqueued.incrementAndGet()
          }
        }
      }
    }
  }

  // TODO need to be thread safe
  def close(): Unit = {
    val itr = segments.values().iterator()
    while(itr.hasNext()){
      val segment = itr.next()
      segment.unmap()
    }
  }

  def stats: QueueStats = {
    QueueStats(queueId,
      numEnqueued.get,
      numCompleted.get,
      numRetried.get,
      segments.values().toList.map(_.stats),
      queue.toList.map(_.stats))
  }

  def incTaskCompleted(): Long = numCompleted.incrementAndGet()
  def incTaskRetry(): Long = numRetried.incrementAndGet()

  def retryRecord(record: Record, timeout: Duration): Boolean =
    queue.offer(record, timeout.toMillis, TimeUnit.MILLISECONDS)

  def flush(): Unit = {
    segments.values().foreach(_.flush())
  }

  override def toString: String = {
    segments.values().map(_.getString()).mkString("\n****** Segment *****\n", "\n****** Segment *****\n", "")
  }

  private def createSegment(): Segment = {
    val id = nextSegmentId.getAndIncrement
    val newSegment = Segment(dirName, segmentSize, id, this)
    if (segments.put(id, newSegment) != null)
      throw new QueueCorruptionException(s"Attempting to create a new segment with pre-existing id: $id")
    newSegment
  }
}
