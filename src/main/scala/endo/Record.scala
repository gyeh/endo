package endo

import java.util.concurrent.atomic.AtomicReference

import Payload.Status._
import endo.Record.RecordStats

import scala.concurrent.duration.Duration

object Record {
  case class RecordStats(offset: Int, length: Int)
  def apply(segment: Segment, queue: EntryQueue, offset: Int, length: Int): Record = {
    new Record(segment, queue, offset, length)
  }
}

/**
 * The external reference for the internal binary Payload object stored within the queue.
 *
 * @param segment Segment file where object is stored
 * @param queue Queue where object is stored
 * @param offset Offset within segment
 * @param length Size of object within segment
 */
class Record(segment: Segment, queue: EntryQueue, val offset: Int, val length: Int) {

  /**
   * Get the binary Payload represented by Record
   */
  def payload: Payload = segment.read(offset, length)

  /**
   * In-memory status reference to avoid mmap seek => must be in-sync with mmap
   * 'statusRef' is the authorative representation of 'status'
   */
  def status: Status = statusRef.get()
  private val statusRef = new AtomicReference[Status](payload.status)

  /**
   * Re-queue Record into parent queue. This is a blocking call.
   *
   * @param timeout Time to wait before timing out
   * @return Success status of queuing operation
   */
  def retry(timeout: Duration): Boolean = {
    setStatus(Unclaimed)
    queue.incTaskRetry()
    queue.retryRecord(this, timeout)
  }

  /**
   * Assert the Record (and underlying Payload) as completed and ready to be deleted from queue.
   */
  def completed(): Unit = {
    setStatus(Completed)
    queue.incTaskCompleted()
  }

  /**
   * Check if Record is completed or not.
   */
  def isCompleted: Boolean = status == Completed

  /**
   * Set the Record status to be "unclaimed" and un-owned by any clients of Endo.
   */
  def unclaimed(): Unit = {
    setStatus(Unclaimed)
  }

  /**
   * Exclusively claim the Record. The calling process has exclusive rights to this record.
   */
  def claimed(): Unit = {
    setStatus(Claimed)
  }

  /**
   * Stats about Record
   */
  def stats: RecordStats = RecordStats(offset, length)

  override def toString: String = payload.toString

  private def setStatus(status: Status): Unit = {
    statusRef.set(status)
    payload.setStatus(status)
  }
}
