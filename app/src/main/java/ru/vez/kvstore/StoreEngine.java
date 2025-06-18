package ru.vez.kvstore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** Handles mmap + serialization. This is a simple key-value store engine that uses memory-mapped files for persistence.
 * It supports basic operations like GET, SET, and DEL.
 * 
 * Key features include:
 * - Managing the persistence layer using MappedByteBuffer.
 * - Implementing a length-prefixed key-value format (Each entry in the file follows this binary layout: [4-byte keyLen][keyBytes][4-byte valLen][valBytes]).
 * - Maintaining an in-memory index (map of key → offset).
 * - Handling GET, SET, DEL commands.
 * 
 * Improvement Ideas:
 * - Add space compaction or log rewrite (like Redis BGREWRITEAOF).
 * - Add force() after writes to flush to disk. Close?.
 * - Store timestamps for TTL.
 * - Track deleted keys for garbage collection.
 * - This is important to ensure that all data is persisted before the application exits.
 */
public class StoreEngine {
    private static final int FILE_SIZE = 1024 * 1024;
    private final MappedByteBuffer buffer;      // the memory-mapped backing file
    private final Map<String, Integer> index = new HashMap<>(); // in-memory index mapping keys to their offsets in the buffer
    private int writeOffset = 0;                // current write position in the buffer

    public StoreEngine(String filename) throws IOException {
        // This maps a file (store.db) directly into memory. You can read/write as if it's RAM, but it's persisted to disk.
        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
            FileChannel channel = raf.getChannel();
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE);
        }
        loadIndex();    
    }

    /** Loads the index from the memory-mapped buffer. It reads key-value pairs and populates the in-memory index.
     * This method assumes the buffer is already populated with valid key-value pairs.
     * The format is:
     * [4-byte keyLen][keyBytes][4-byte valLen][valBytes]
     * 
     * It only reads records that are fully written.
     * Index maps keys to their offset in the file (start of record).
     * writeOffset tracks the next available space to append.
     */
    private void loadIndex() {
        buffer.position(0);
        while (buffer.remaining() >= 8) {
            int keyLen = buffer.getInt();
            if (keyLen <= 0 || buffer.remaining() < keyLen + 4) break;
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            int valLen = buffer.getInt();
            if (valLen < 0 || buffer.remaining() < valLen) break;
            byte[] valBytes = new byte[valLen];
            buffer.get(valBytes);

            String key = new String(keyBytes, StandardCharsets.UTF_8);
            index.put(key, buffer.position() - valLen - 4 - keyLen - 4);
            writeOffset = buffer.position();
        }
    }

    /* Finds the offset from index. Reads the key/value from the buffer starting at that offset.
     * Reconstructs the string value and returns it.
     */
    public synchronized String get(String key) {
        Integer offset = index.get(key);
        if (offset == null) return null;

        buffer.position(offset);
        int keyLen = buffer.getInt();
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);
        int valLen = buffer.getInt();
        byte[] valBytes = new byte[valLen];
        buffer.get(valBytes);

        return new String(valBytes, StandardCharsets.UTF_8);
    }

    /** Appends a new key-value pair in the store at writeOffset.
     * It appends the new entry to the end of the buffer and updates the index to point to this new location.
     * Does not erase older entries (like Redis AOF — old values persist but are no longer indexed).
     * Important: This model is append-only. Deletion does not reclaim space immediately.
     * 
     * The format is:
     * [4-byte keyLen][keyBytes][4-byte valLen][valBytes]
     * 
     * Throws RuntimeException if there is not enough space in the buffer.
     */
    public synchronized void set(String key, String value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);

        if (writeOffset + 4 + keyBytes.length + 4 + valBytes.length > buffer.capacity())
            throw new RuntimeException("Buffer full");

        buffer.position(writeOffset);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valBytes.length);
        buffer.put(valBytes);

        index.put(key, writeOffset);
        writeOffset = buffer.position();
    }

    /** Deletes a key from the store. Removes the key from the in-memory index but does not erase the data from the buffer.
     * The data remains in the buffer, but it is no longer accessible via GET.
     * 
     * Returns true if the key was found and removed, false otherwise.
     */
    public synchronized boolean del(String key) {
        return index.remove(key) != null;
    }
}
