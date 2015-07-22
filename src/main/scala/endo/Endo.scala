package endo

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.{TimerTask, Timer}

import com.typesafe.scalalogging.Logger
import endo.EntryQueue.QueueStats
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import collection.JavaConverters._

object Endo {

  /**
   * @param segmentSize Size of each mmap file
   * @param fsyncInterval Frequency of fsync of mmap files
   * @param fsyncEnabled Fsync to disk after every write
   */
  case class Options(segmentSize: Int, fsyncInterval: Option[Duration], fsyncEnabled: Boolean)
  val defaultOptions = Options(64 * 1024 * 1024, Some(1.minute), fsyncEnabled = false)
  val defaultId = "default"

  private val logger = Logger(LoggerFactory.getLogger(Endo.getClass))

  /**
   * Endo Factory
   *
   * @param dirName Location of on-disk files
   * @param maxEntries Max bounded size of queue
   * @param opt Endo-specific options
   * @return An Endo instance
   */
  def apply(dirName: String, maxEntries: Int, opt: Options = defaultOptions): Endo = {
    new Endo(dirName, maxEntries, opt)
  }

  private class LazyEntryQueue(value: => EntryQueue) extends Proxy {
    override def self = value
    lazy val get: EntryQueue = value
  }
}

class Endo(dirName: String, maxEntries: Int, opt: Endo.Options) {
  import Endo._

  private val queueMap = new ConcurrentHashMap[String, LazyEntryQueue]().asScala

  private val dir = {
    val d = new File(dirName)
    if (!d.exists()) d.mkdir()
    preInitQueues()
    d
  }

  /**
   * Background job to flush mmap segments to disk.
   */
  private val flushTimer: Option[Timer] = {
    for {
      interval <- opt.fsyncInterval
    } yield {
      val task = new TimerTask{
        override def run(): Unit = {
          queueMap.values.foreach(_.get.flush())
        }
      }

      val interval = opt.fsyncInterval.getOrElse(120.seconds).toMillis
      val t = new Timer(true)
      t.scheduleAtFixedRate(task, interval, interval)
      t
    }
  }

  /**
   * Blocking call to add Payload to beginning of queue.
   * @param payload Payload to add
   * @param timeout Time to wait for blocking call
   * @param queueId Unique identifer for queue
   * @return Indication of operation success or failure
   */
  def offer(payload: Payload, timeout: Duration = Duration.Inf, queueId: String = Endo.defaultId): Boolean = {
    val queue = getOrElseUpdate(queueId, EntryQueue(queueId, maxEntries, dirName, opt.segmentSize))
    queue.offer(payload, opt.fsyncEnabled, timeout)
  }

  /**
   * Blocking call to retrieve record from end of queue.
   * @param timeout Time to wait for blocking call.
   * @param queueId Unique identifer for queue
   * @return Record type of payload.
   */
  def poll(timeout: Duration = Duration.Inf, queueId: String = Endo.defaultId): Option[Record] = {
    queueMap.get(queueId) match {
      case Some(queue) =>
        queue.get.poll(timeout)
      case None =>
        logger.info(s"queueId, $queueId, does not exist.")
        None
    }
  }

  /**
   * Properly close each queue and unmap each segment.
   */
  def close(): Unit = {
    flushTimer.foreach(_.cancel())

    queueMap.foreach { case((_, queue)) =>
      queue.get.close()
    }
  }

  /**
   * Stats object reflecting queue state.
   */
  def stats: Seq[QueueStats] = {
    queueMap.values.toList.map(_.get.stats)
  }

  override def toString: String = {
    queueMap.values.toList.map(_.get.toString()).mkString("\n===== Queue =====\n", "\n===== Queue =====\n", "")
  }

  // necessary because scala stdlib "getOrElseUpdate" is not atomic
  // TODO: use computeifabsent from java 8 or use scala atomic variant
  private def getOrElseUpdate(key: String, op: => EntryQueue): EntryQueue = {
    val lazyOp = new LazyEntryQueue(op)
    queueMap.putIfAbsent(key, lazyOp).getOrElse(lazyOp).get
  }

  /**
   * Initialize pre-existing queues from on-disk files
   */
  private def preInitQueues(): Unit = {
    new File(dirName).listFiles()
      .filter(_.isDirectory)
      .map(d => EntryQueue(d.getName, maxEntries, dirName, opt.segmentSize))
      .foreach { q =>
        q.loadFromDisk()
        queueMap.put(q.queueId, new LazyEntryQueue(q))
      }
  }

}



