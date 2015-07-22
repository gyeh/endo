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

  case class Options(segmentSize: Int, fsyncInterval: Option[Duration], fsyncEnabled: Boolean)
  val defaultOptions = Options(64 * 1024 * 1024, Some(1.minute), fsyncEnabled = false)
  val defaultId = "default"

  private val logger = Logger(LoggerFactory.getLogger(Endo.getClass))

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

  // TODO: prob don't want to convert this
  private val queueMap = new ConcurrentHashMap[String, LazyEntryQueue]().asScala

  private val dir = {
    val d = new File(dirName)
    if (!d.exists()) d.mkdir()
    preInitQueues()
    d
  }

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

  def offer(payload: Payload, timeout: Duration = Duration.Inf, queueId: String = Endo.defaultId): Boolean = {
    val queue = getOrElseUpdate(queueId, EntryQueue(queueId, maxEntries, dirName, opt.segmentSize))
    queue.offer(payload, opt.fsyncEnabled, timeout)
  }

  def poll(timeout: Duration = Duration.Inf, queueId: String = Endo.defaultId): Option[Record] = {
    queueMap.get(queueId) match {
      case Some(queue) =>
        queue.get.poll(timeout)
      case None =>
        logger.info(s"queueId, $queueId, does not exist.")
        None
    }
  }

  def close(): Unit = {
    flushTimer.foreach(_.cancel())

    queueMap.foreach { case((_, queue)) =>
      queue.get.close()
    }
  }

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



