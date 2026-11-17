# Technical Approach

This document explains the design, protocol, and implementation decisions for the message relay service.

---

## 1. Requirements Checklist

| Requirement | How it is handled |
|---|---|
| **1. Multiple Clients** | Multiple clients can register concurrently with a unique name (`clientId`). |
| **2. Sending Messages** | Any client can send a message to another client using a message ID (`msgId`). |
| **3. Send Confirmation** | The server replies with `SEND_ACK` (`ACCEPTED` or `REJECTED` with a reason). |
| **4. Delivery & ACK** | The server pushes `DELIVER` to the recipient. The message stays on the server until the recipient sends an `ACK`. |
| **5. Offline Retention** | If a recipient is offline, messages wait in its mailbox up to the limit (`1000` messages). |
| **6. Reconnecting** | When a client reconnects and registers with the same name, any waiting messages are sent immediately. |
| **7. Redelivery** | If a client disconnects before sending an `ACK`, that message is redelivered when they reconnect. |
| **Bonus 1: FIFO Ordering** | Messages are delivered in first-in, first-out order. On reconnect, unacknowledged messages are placed back at the front of the queue so the original order is preserved. |
| **Bonus 2: Docker** | Includes a multi-stage `Dockerfile` with a minimal runtime image and volume support for persistent storage. |
| **Bonus 3: State at Rest** | Optional Write-Ahead Log (`WalStorage`). Appends new messages and acknowledgments to disk. On restart, replaying the log restores all unacknowledged messages. |

---

## 2. Protocol Design

Messages are exchanged over TCP as simple, one-line JSON strings ending with a newline (`\n`).

### Client to Server Messages

1. **Register**:
   ```json
   {"type":"REGISTER","clientId":"batman"}
   ```
2. **Send a Message**:
   ```json
   {"type":"SEND","msgId":"m1","recipientId":"robin","payload":"Bat-signal is on!"}
   ```
3. **Acknowledge Delivery**:
   ```json
   {"type":"ACK","msgId":"m1"}
   ```

### Server to Client Messages

1. **Registration Success**:
   ```json
   {"type":"REGISTERED","clientId":"batman"}
   ```
2. **Send Confirmation**:
   ```json
   {"type":"SEND_ACK","msgId":"m1","status":"ACCEPTED"}
   {"type":"SEND_ACK","msgId":"m1","status":"REJECTED","reason":"MAILBOX_FULL"}
   ```
3. **Message Delivery**:
   ```json
   {"type":"DELIVER","msgId":"m1","senderId":"batman","payload":"Bat-signal is on!"}
   ```
4. **Error**:
   ```json
   {"type":"ERROR","code":"UNREGISTERED","message":"You must REGISTER first"}
   ```

### Error Codes
- `INVALID_FRAME`: Malformed JSON or unreadable data.
- `UNREGISTERED`: Tried to send or ack before registering.
- `INVALID_FIELD`: Missing required fields like `msgId` or `payload`.
- `PAYLOAD_TOO_LARGE`: Line exceeded the 16 KB size limit.
- `CONNECTION_LIMIT_EXCEEDED`: Too many active connections (limit: 100).
- `MAILBOX_FULL`: Recipient mailbox has reached its limit (limit: 1000).
- `DUPLICATE_MSG_ID`: The sender already sent a message with this `msgId`.
- `STORAGE_ERROR`: Disk write failure when persistence is enabled.

---

## 3. How the Code Works

### Threading with Java 21 Virtual Threads
- When a client connects, the server starts a Java 21 virtual thread to handle that connection.
- Virtual threads are lightweight, so we can have many clients without using up system memory.

### Per-Mailbox Locks (`ReentrantLock`)
- Instead of using a global lock for the whole server, each client has their own `ClientMailbox` with its own `ReentrantLock`.
- When Batman sends a message to Robin:
  - The server only locks Robin's mailbox for a fraction of a millisecond to add the message.
  - Other clients (like Alfred) are not locked or delayed.
- Why `ReentrantLock` instead of `synchronized`?
  - Explicit lock and unlock boundaries (`lock.lock()` ... `lock.unlock()`).
  - Clear critical sections that are easy to inspect and maintain.
  - Readily extensible if timeout-based locking (`tryLock`) or condition variables (`Condition`) are needed.

### Strict FIFO Ordering
- Messages are stored in a deque (`ArrayDeque`).
- When delivered, messages move to an in-flight map (`LinkedHashMap`) which preserves insertion order.
- If a client disconnects before acknowledging, unacknowledged in-flight messages are prepended back to the front of the pending queue in their original sequence.
- Any messages that arrived while the client was offline wait behind the redelivered messages, guaranteeing exact FIFO order upon reconnect.

### State at Rest & Resilience (`WalStorage`)
- When a storage directory is configured (`dataDir`), the server writes to an append-only log file (`wal.log`).
- **On Send**: The message is written to disk with `{"op":"ENQUEUE", ...}` and flushed before entering memory.
- **On Ack**: When the recipient acknowledges, `{"op":"ACK", ...}` is appended to the log.
- **On Server Restart**: The server reads `wal.log` from top to bottom. It reconstructs every client mailbox with only active, unacknowledged messages in their original FIFO order.
- **Compaction**: On clean shutdown, the server rewrites the log with only active messages, discarding old historical ACKs so disk space stays bounded.
- **Fault Handling**: If a disk write fails, the server rejects the send with `STORAGE_ERROR` to protect data integrity.

---

## 4. Edge Cases & Safety

### 1. Large Messages and Malformed Input
- A bad client could try to send an endless line without a newline to crash the server with an `OutOfMemoryError`.
- `BoundedFrameReader` counts bytes as it reads. If a line exceeds 16 KB without a newline, it stops, sends `PAYLOAD_TOO_LARGE`, and closes the connection.

### 2. Duplicate Message IDs
- If Batman sends message `m1`, and then sends `m1` again, the server rejects the second send with `DUPLICATE_MSG_ID`.
- We track recently seen message IDs in a bounded cache (up to 50,000 entries) so memory stays bounded.

### 3. Extra or Stale ACKs
- If Robin sends an `ACK` for an ID that was already acknowledged or does not exist, the server simply ignores it. It does not crash or throw an error.

### 4. Connection Limits
- A `Semaphore` limits active connections to 100.
- If a 101st client connects, the server sends `CONNECTION_LIMIT_EXCEEDED` and closes the socket immediately.

---

## 5. Automated Tests

All tests run using a single command:
```bash
mvn test
```

### What the Tests Cover
The test suite contains 15 automated tests covering:
1. **Send and Receive**: Batman registers, Robin registers, Batman sends a message, Robin receives it, Robin ACKs, and the mailbox is empty.
2. **Offline Delivery**: Robin is offline, Batman sends 3 messages, Robin connects, and receives all 3 in order.
3. **Redelivery after Disconnect**: Robin receives 2 messages, ACKs the first, and disconnects before acking the second. On reconnect, the second message is redelivered first.
4. **Strict FIFO Ordering**: Verifies that 10 interleaved messages sent to offline Robin are delivered in exact numerical sequence upon connection.
5. **Disk Persistence Across Restarts**: Batman sends messages to offline Robin on Server 1. Server 1 shuts down. A brand new Server 2 starts with the same data directory. Robin connects and receives the messages in exact order. Acknowledged messages do not reappear after subsequent restarts.
6. **Resource Limits**: Verifies rejections for full mailboxes, oversized messages, and connection limits.
7. **Edge Cases**: Verifies handling of duplicate sends, stale ACKs, malformed JSON, and ensures a slow client does not block other clients.

### What the Tests Do Not Cover (And Why)
- **Multi-Node Cluster / Network Partitions**: The exercise defines a single-process relay service, so split-brain scenarios and network partition chaos testing are intentionally out of scope.
- **TLS / Packet Encryption**: Transport-layer security was explicitly excluded per scope instructions to keep networking logic transparent and focused on core relay mechanics.
- **Large-Scale Soak / Stress Testing**: We chose deterministic, fast-running unit and socket integration tests bounded by strict timeouts over long-running benchmark suites.

---

## 6. Trade-offs & Future Improvements

### Trade-offs & Shortcuts Made
- **Clean Shutdown Compaction**: WAL compaction happens on clean server shutdown rather than in an active background worker thread, keeping the concurrency model simple and deterministic.
- **Text-based Log File**: Messages are saved as readable JSON lines in `wal.log`. This makes debugging straightforward, but uses slightly more disk space than a binary format.
- **Socket Inactivity Timeout**: The server relies on TCP socket timeouts (`30s`) to detect dropped connections rather than a custom application-level ping/pong heartbeat.

### Future Improvements
- **Application Heartbeats**: Add ping/pong frames to detect dropped connections faster than waiting for TCP socket read timeouts.
- **Periodic Background Compaction**: Trigger log compaction periodically while the server is running if the log exceeds a certain file size.
- **Database Storage**: Replace the flat file log with SQLite or a database if relational queries or indexing are needed.

---

## 7. AI Tool Usage

AI was used as an interactive sounding board during development:
- To quickly brainstorm the JSON protocol format.
- To design the Write-Ahead Log (WAL) append and recovery structure.
- To discuss the clean way to handle unacknowledged messages during reconnects (putting them back at the front of the queue).
- All generated code was verified by running automated tests against real sockets.
