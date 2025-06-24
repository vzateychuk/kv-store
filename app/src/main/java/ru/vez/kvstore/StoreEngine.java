package ru.vez.kvstore;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/** Handles mmap + serialization. This is a simple key-value store engine that uses memory-mapped files for persistence.
 * It supports basic operations like GET, SET, and DEL.
 * 
 * Key features include:
 * - Managing the persistence layer using MappedByteBuffer.
 * - Implementing a length-prefixed key-value format (Each entry in the file follows this binary layout: [4-byte keyLen][keyBytes][4-byte valLen][valBytes][8-byte expireTs]).
 *   Where:
 *      - keyLen: length of the key in bytes
 *      - keyBytes: the actual key bytes
 *      - valLen: length of the value in bytes
 *      - valBytes: the actual value bytes
 *      - expireTs: timestamp in milliseconds when the key should expire (0 means no expiration)
 * - Maintaining an in-memory index (map of key → offset).
 * - Handling GET, SET, DEL commands.
 * 
 * Improvement Ideas:
 * - Add space compaction or log rewrite (like Redis BGREWRITEAOF).
 * - Add force() after writes to flush to disk. Close?.
 * - Track deleted keys for garbage collection.
 * - This is important to ensure that all data is persisted before the application exits.
 */
public class StoreEngine {
    private static final int FILE_SIZE = 1024 * 1024;
    private static final int HEADER_SIZE = 4 + 4 + 8; // keyLen + valLen + expireTs
    
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
     * [4-byte keyLen][keyBytes][4-byte valLen][valBytes][8-byte expireTs]
     *
     * It only reads records that are fully written.
     * Index maps keys to their offset in the file (start of record).
     * writeOffset tracks the next available space to append.
     * loadIndex() skips expired entries during startup.
     */
    private void loadIndex() {
        buffer.position(0);
        while (buffer.remaining() >= HEADER_SIZE) {
            int pos = buffer.position();
            int keyLen = buffer.getInt();
            if (keyLen <= 0 || buffer.remaining() < keyLen + 4 + 8) break;
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);

            int valLen = buffer.getInt();
            if (valLen < 0 || buffer.remaining() < valLen + 8) break;
            byte[] valBytes = new byte[valLen];
            buffer.get(valBytes);

            long expireTs = buffer.getLong();
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            long now = System.currentTimeMillis();
            if (expireTs == 0 || expireTs > now) {
                index.put(key, pos);
            }
            writeOffset = buffer.position();
        }
    }

    /* Finds the offset from index. Reads the key/value from the buffer starting at that offset.
     * - reconstructs the string value and returns it.
     * - checks and evicts expired entries.
     * 
     * @param key the key to retrieve
     * @return the value associated with the key, or null if the key does not exist
     * @throws IllegalArgumentException if the key is blank
     */
    public synchronized String get(String key) {
        if (StringUtils.isBlank(key)) throw new IllegalArgumentException("key must not be blank");
    
        Integer offset = index.get(key);
        if (offset == null) return null;

        buffer.position(offset);
        int keyLen = buffer.getInt();
        buffer.position(buffer.position() + keyLen); // skip key
        int valLen = buffer.getInt();
        byte[] valBytes = new byte[valLen];
        buffer.get(valBytes);
        long expireTs = buffer.getLong();

        // checks and evicts expired entries.
        long now = System.currentTimeMillis();
        if (expireTs != 0 && expireTs < now) {
            index.remove(key);
            return null;
        }
        return new String(valBytes, StandardCharsets.UTF_8);
    }

    /** Appends a new key-value pair in the store at writeOffset.
     * It appends the new entry to the end of the buffer and updates the index to point to this new location.
     * Does not erase older entries (like Redis AOF — old values persist but are no longer indexed).
     * Accepts a ttlMillis argument (0 = no expiry).
     * Important: This model is append-only. Deletion does not reclaim space immediately.
     * 
     * @param key the key to store
     * @param value the value to store
     * @param ttlMillis time-to-live in milliseconds (0 means no expiration)
     * @throws IllegalArgumentException if the key is blank or ttlMillis is negative
     * @throws NullPointerException if the value is null
     * @throws RuntimeException if there is not enough space in the buffer (the buffer is full or if the key/value sizes exceed the remaining space).
     */
    public synchronized void set(String key, String value, long ttlMillis) {
        if (StringUtils.isBlank(key)) throw new IllegalArgumentException("key must not be blank");
        if (value == null) throw new NullPointerException("value must not be null");

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
        long expireTs = ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : 0;

        int entrySize = HEADER_SIZE + keyBytes.length + valBytes.length;
        if (writeOffset + entrySize > buffer.capacity())
            throw new RuntimeException("Buffer full");

        buffer.position(writeOffset);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valBytes.length);
        buffer.put(valBytes);
        buffer.putLong(expireTs);

        index.put(key, writeOffset);
        writeOffset = buffer.position();
    }

    /** Deletes a key from the store. Removes the key from the in-memory index but does not erase the data from the buffer.
     * The data remains in the buffer, but it is no longer accessible via GET.
     * 
     * Returns true if the key was found and removed, false otherwise.
     * @param key the key to delete
     * @return true if the key was found and removed, false if the key does not exist
     * @throws IllegalArgumentException if the key is blank
     */
    public synchronized boolean del(String key) {
        if (StringUtils.isBlank(key)) throw new IllegalArgumentException("key must not be blank");
        return index.remove(key) != null;
    }

    /** Updates the TTL (expire time) for an existing key.
     * If the key exists, appends a new record with the same value and new expiration.
     * Returns true if the key existed and was updated, false otherwise.
     * This method does not remove the old entry; it simply adds a new one with the updated expiration.

     * @param key the key to update
     * @param ttlMillis the new time-to-live in milliseconds (0 means no expiration)
     * @return true if the key was found and updated, false if the key does not exist

     * @throws NullPointerException if the key is null
     * @throws IllegalArgumentException if key is blank or ttlMillis is negative
     * @throws RuntimeException if there is not enough space in the buffer to write the new entry
     */
    public synchronized boolean expire(String key, long ttlMillis) {

        if (StringUtils.isBlank(key)) throw new IllegalArgumentException("key must not be blank");
        if (ttlMillis < 0) throw new IllegalArgumentException("ttlMillis must not be negative");
        
        Integer offset = index.get(key);
        if (offset == null) return false;

        buffer.position(offset);
        int keyLen = buffer.getInt();
        buffer.position(buffer.position() + keyLen); // skip key
        int valLen = buffer.getInt();
        byte[] valBytes = new byte[valLen];
        buffer.get(valBytes);
        // skip old expireTs
        buffer.getLong();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        long expireTs = ttlMillis > 0 ? System.currentTimeMillis() + ttlMillis : 0;
        int entrySize = HEADER_SIZE + keyBytes.length + valBytes.length;
        if (writeOffset + entrySize > buffer.capacity())
            throw new RuntimeException("Buffer full");

        buffer.position(writeOffset);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valBytes.length);
        buffer.put(valBytes);
        buffer.putLong(expireTs);

        index.put(key, writeOffset);
        writeOffset = buffer.position();
        return true;
    }   
}
