# kv-store
Redis-like KV Store with IPC + Memory Mapping

## ✅ Recap: Goals
- Build a Redis-style key-value store:
- Uses memory mapping (i.e. Memory-mapped files for efficient memory usage and persistence).
- Uses Multi-process communication (IPC) (via Unix domain sockets, or alternatives).
- Supports SET, GET, DEL.
- Multi-client capable.
- Efficient and persistent.

## 🧠 Architecture Plan
1. Core Modules
    a. KeyValueServer
    - Manages memory-mapped file for in-memory persistence.
    - Listens for client connections over IPC.
    - Parses commands, updates in-memory data.
    b. KeyValueClient
    - Sends commands via IPC to the server.
    - Receives and prints responses.

## 🛠️ Development Steps

### Phase 1: Setup Memory-Mapped File
1. Create or open a file for backing store (e.g. "store.db").
2. Use `RandomAccessFile` + `FileChannel.map()` to map it into memory.
3. Allocate a fixed-size region (e.g., 1MB to start).
4. Use `MappedByteBuffer` to read/write to the mapped area.
```java
FileChannel channel = new RandomAccessFile("store.db", "rw").getChannel();
MappedByteBuffer buffer = channel.map(MapMode.READ_WRITE, 0, 1024 * 1024);
```

### Phase 2: IPC Server Setup
Option 1: TCP Socket (simpler)
- Use `ServerSocket` and Socket for connections.
```java
ServerSocket serverSocket = new ServerSocket(12345);
Socket client = serverSocket.accept();
```

Option 2: Unix Domain Socket (for true IPC)
- Use `junixsocket`:
```xml
<dependency>
  <groupId>com.kohlschutter.junixsocket</groupId>
  <artifactId>junixsocket-core</artifactId>
  <version>2.6.0</version>
</dependency>
```

### Phase 3: Command Protocol
Text-based (like Redis):
```bash
SET foo bar\n
GET foo\n
DEL foo\n
```

### Phase 4: Data Storage Design and Data Structure
Since memory-mapped files can't store Java objects directly, you must:
- Serialize key-value pairs manually (e.g. length-prefixed encoding).
- Implement a custom serialization format, e.g.:
```text
[4-byte keyLen][keyBytes][4-byte valLen][valBytes]
```
- Keep a hash map in-memory to index into offsets in the buffer
```java
Map<String, Integer> keyOffsetMap; // maps key to byte offset: key → byte offset in MappedByteBuffer
```
⚠️ You must rebuild this index on startup by scanning the buffer.

### Phase 5: Server Event Loop
- Accept client connections.
- Read commands line-by-line.
- Parse the base command (`SET`, `GET`, `DEL`).
- Lookup or store into `MappedByteBuffer`.
- Send response back to client.

### Phase 6: Client Interface
- Connect to server via socket (Use Socket or AFUNIXSocket).
- Write commands via OutputStream.
- Read responses via InputStream.
> Consider a CLI REPL-style tool or a simple Client.send(String command) class.


### Phase 7. Optimizations and Persistence
- Periodically sync memory to file. Call `MappedByteBuffer.force()` to flush memory to disk periodically or after every SET/DEL:
```java
buffer.force(); // flushes dirty pages to disk
```
- Expiration / TTL. Store timestamps (record format: `[keyLen][key][valLen][val][expireTs]`) and run background cleanup (Run a background thread to check and delete expired entries).
- Concurrency Control. Start with a single-threaded event loop or thread-per-client using a `ThreadPoolExecutor`.
    - Start with synchronous single-threaded loop.
    - Upgrade to `ThreadPoolExecutor` or NIO Selector if needed.
    - Add synchronized blocks or locks if you go multithreaded.
- Consider handling additional command `UPDATE`, `EXISTS`, etc., later.

