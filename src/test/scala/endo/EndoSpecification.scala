package endo

import java.io.File
import java.nio.ByteBuffer

import org.scalatest._

import scala.concurrent.duration.Duration

class EndoSpecification extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val dirName = "test_endo"

  def removeAll(path: String) = {
    def getRecursively(f: File): Seq[File] =
      if (f == null) Seq()
      else f.listFiles.filter(_.isDirectory).flatMap(getRecursively) ++ f.listFiles

    getRecursively(new File(path)).foreach{f =>
      if (!f.delete())
        throw new RuntimeException("Failed to delete " + f.getAbsolutePath)}
  }

  "Endo" should "retrieve queued entries" in {
    removeAll(dirName)
    val endo = Endo(dirName = dirName, maxEntries = 100, Endo.defaultOptions.copy(segmentSize = 60, fsyncEnabled = false))

    val payload = Payload(ByteBuffer.wrap("testValue".getBytes))
    val payload2 = Payload(ByteBuffer.wrap("testValue2".getBytes))
    endo.offer(payload)
    endo.offer(payload2)

    val r = endo.poll().get
    r.offset shouldEqual 0
    r.length shouldEqual 24

    val r2 = endo.poll().get
    r2.offset shouldEqual 24
    r2.length shouldEqual 25
  }

  "Endo" should "not retrieve non-queued entries" in {
    removeAll(dirName)
    val endo = Endo(dirName = dirName, maxEntries = 100)
    endo.poll(Duration(100, "millis")) shouldEqual None
  }

  "Endo" should "return stats reflecting queue state" in {
    removeAll(dirName)
    val endo = Endo(dirName = dirName, maxEntries = 100)
    endo.stats.length shouldEqual 0

    val payload = Payload(ByteBuffer.wrap("testValue".getBytes))
    endo.offer(payload)
    endo.stats.length should be > 0
  }
}

