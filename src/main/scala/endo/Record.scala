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
class Record(segment: Segment, queue: EntryQueue, val offset: Int, val length: Int) {
  def payload: Payload = segment.read(offset,length)
  // in-memory status reference to avoid mmap seek => must be in-sync with mmap
  // 'statusRef' is the authorative representation of 'status'
  private val statusRef = new AtomicReference[Status](payload.status)

  def status: Status = statusRef.get()

  def retry(timeout: Duration): Boolean = {
    setStatus(Unclaimed)
    queue.incTaskRetry()
    queue.retryRecord(this, timeout)
  }
  // record in underlying payload of record status
  def completed(): Unit = {
    setStatus(Completed)
    queue.incTaskCompleted()
  }

  def isCompleted: Boolean = status == Completed

  // record in underlying payload of record status
  def unclaimed(): Unit = {
    setStatus(Unclaimed)
  }

  def claimed(): Unit = {
    setStatus(Claimed)
  }

  def stats: RecordStats = RecordStats(offset, length)

  private def setStatus(status: Status): Unit = {
    statusRef.set(status)
    payload.setStatus(status)
  }
}
