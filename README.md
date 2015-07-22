# Endo

Endo is a thread-safe, on-disk blocking queue for the JVM, with simple 'offer'/'poll' semantics. 
It is useful for long-lived queuing operations within volatile environments or applications with 
stronger durability requirements (e.g. an on-disk buffer to store outgoing payloads when the
network goes down).

Endo is reasonably fast -- backed by memory-map files, with configurable fsync strategies.

## Sample Usage

```scala
import endo.Endo

val options = Options(segmentSize = 64 * 1024 * 1024, 
                      fsyncInterval = Some(1.minute), 
                      fsyncEnabled = false)
                      
val endo = Endo(dirName = "queue_files", maxEntries = 100, opt = options)

// Offer is a blocking write. It will block if the queue is full. 
// A timeout can be configured.
endo.offer(Payload(ByteBuffer.wrap("testValue".getBytes)))

// Poll is a blocking read, waiting until an entry is available or a timeout occurs.
// If a Record is retrieved, the record is exclusively owned by the caller process 
// and no other process can manipulate the record.
val record: Record = endo.poll().get

// Get the binary blob referenced by the Record
val payload: ByteBuffer = record.payload.buffer 

// Specify that the Record is finished and can be removed from disk.
record.completed 

// Re-offer the Record into the front of the queue
record.retry 

// Caller process gives up exclusive ownership of Record
record.unclaimed
```

## Design

The Endo queue is internally represented by a series of memory-mapped files, which contains the 
actual binary payloads of queue entries. New entries are 'offered' into a single mmap'd file until the
configured max size is reached. Subsequently a new mmap'd file is created. 

The memory-mapped files can be fsynced through two different ways: 
  1. An option to fsync after every write 
  2. A configurable background job to fsync all mmap'd files to disk every X seconds.
  
In addition, there is an in-memory queue which contains the in-memory references that 
corresponds 1:1 to the mmap'd binary payloads. This in-memory queue effectively provides the
external 'offer'/'poll' queue semantics to the external user. Note for 'poll', the read never goes 
to disk and always returns an in-memory reference. One can always subsequently dereference the Record
to access the underlying binary payload.

During initialization, Endo will attempt to rebuild both the mmap and in-memory queue from the disk files.
All disk reads are verified with a checksum to guard against possible disk corruption.

