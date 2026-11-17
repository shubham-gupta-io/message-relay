# Client/Server Message Relay

A simple message relay service built with Java 21. It lets clients connect over TCP, send messages to each other, disconnect, reconnect, and receive queued messages in order.

---

## 1. Prerequisites

- **Java**: Java 21 or later (`java -version`)
- **Maven**: Maven 3.9 or later (`mvn -version`)
- **Docker** (optional, for running in a container): Docker 20+
- **Netcat** (optional, for testing): `nc`

---

## 2. Quick Start

### Run the Tests
```bash
mvn test
```
Runs all 15 automated tests covering registration, message send/delivery, explicit ACKs, reconnects, offline queueing, strict FIFO ordering, and disk persistence across server restarts.

### Build the Application
```bash
mvn clean package
```
Builds a single executable JAR file at:
```
target/message-relay-1.0.0.jar
```

### Run the Server
```bash
# Run in-memory on default port 8080
java -jar target/message-relay-1.0.0.jar

# Run with persistence enabled (messages saved to ./data)
java -jar target/message-relay-1.0.0.jar 8080 ./data

# Or configure via environment variables
PORT=9090 DATA_DIR=./data java -jar target/message-relay-1.0.0.jar
```

---

## 3. Running with Docker

You can build and run the service in a container using the included multi-stage `Dockerfile`:

### Build the Image
```bash
docker build -t message-relay:1.0.0 .
```

### Run the Container
```bash
# Run with persistent volume mounted
docker run -d -p 8080:8080 -v $(pwd)/data:/data --name message-relay message-relay:1.0.0
```

---

## 4. Configuration & Limits

All limits have safe defaults in `RelayConfig`:

| Setting | Default | What it does |
|---|---|---|
| `port` | `8080` | Port the server listens on. |
| `maxActiveConnections` | `100` | Max clients connected at the same time. Extra connections are rejected. |
| `maxMailboxCapacity` | `1000` | Max messages in a client's mailbox. Sends are rejected if full. |
| `maxFrameBytes` | `16 KB` | Max size for a single message line. Prevents memory issues from bad clients. |
| `socketTimeoutMs` | `30s` | Socket read timeout to clean up dead connections. |
| `dataDir` | `null` | Storage directory for WAL persistence across restarts. When omitted, runs in memory. |

### Dependencies & Lifecycle
- **Dependencies Control**: Minimal third-party footprint limited strictly to `jackson-databind` (for JSON serialization), `slf4j-simple` (for logging), and `junit-jupiter` + `awaitility` (test scope only). No frameworks (Spring/Netty) or messaging brokers.
- **Server Lifecycle**: Managed cleanly via an active JVM shutdown hook (`Runtime.getRuntime().addShutdownHook(...)`), which closes the listening socket, stops active client connections, compacts the WAL log, and cleanly terminates virtual thread executors within a 3-second grace window.

---

## 5. Try it with Netcat (`nc`)

Open two terminal windows:

### Terminal 1: Robin Connects
```bash
nc 127.0.0.1 8080
```
Register as `robin`:
```json
{"type":"REGISTER","clientId":"robin"}
```
Server replies:
```json
{"type":"REGISTERED","clientId":"robin"}
```

### Terminal 2: Batman Sends a Message
```bash
nc 127.0.0.1 8080
```
Register and send:
```json
{"type":"REGISTER","clientId":"batman"}
{"type":"SEND","msgId":"m1","recipientId":"robin","payload":"Bat-signal is on!"}
```
Batman gets confirmation:
```json
{"type":"SEND_ACK","msgId":"m1","status":"ACCEPTED"}
```
Robin (in Terminal 1) receives:
```json
{"type":"DELIVER","msgId":"m1","senderId":"batman","payload":"Bat-signal is on!"}
```

### Robin Acknowledges
In Terminal 1 (Robin), type:
```json
{"type":"ACK","msgId":"m1"}
```
The message is now removed from the server (and marked acknowledged in the persistent log).
