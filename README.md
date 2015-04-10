# Endo

Endo is a concurrent, boundeded, on-disk blocking queue for the JVM with simple 'offer'/'poll' semantics. Useful for long-lived queuing operations within volatile environments.

Endo is reasonably fast -- backed by memory-map files, with configurable fsync strategies.

## Sample Usage

```scala
import endo.Endo

val endo = Endo(dirName = "queue_files", maxEntries = 100)
endo.offer(Payload(ByteBuffer.wrap("testValue".getBytes)))
val record = endo.poll().get

record.completed // specify that the task is finished and can be removed from queue
```
