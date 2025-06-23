package ru.vez.kvstore;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StoreEngineTest {

    private File tempFile;
    private StoreEngine store;

    @BeforeEach
    void setUp() throws Exception {
        tempFile = Files.createTempFile("kvstore", ".db").toFile();
        store = new StoreEngine(tempFile.getAbsolutePath());
    }

    @AfterEach
    void tearDown() {
        tempFile.delete();
    }

    
    @Test
    void testSetAndGet() {
        store.set("foo", "bar", 0);
        assertEquals("bar", store.get("foo"));
    }

        @Test
    void testMultipleKeys() {
        store.set("a", "1", 0);
        store.set("b", "2", 0);
        store.set("c", "3", 0);
        assertEquals("1", store.get("a"));
        assertEquals("2", store.get("b"));
        assertEquals("3", store.get("c"));
    }

    @Test
    void testOverwriteValue() {
        store.set("key", "value1", 0);
        store.set("key", "value2", 0);
        assertEquals("value2", store.get("key"));
    }

    
    @Test
    void testDelete() {
        store.set("delkey", "value", 0);
        assertTrue(store.del("delkey"));
        assertNull(store.get("delkey"));
        assertFalse(store.del("delkey"));
    }

    @Test
    void testExpiration() throws InterruptedException {
        store.set("expkey", "expvalue", 100); // 100 ms TTL
        assertEquals("expvalue", store.get("expkey"));
        Thread.sleep(150);
        assertNull(store.get("expkey"));
    }

    @Test
    void testNonExistentKey() {
        assertNull(store.get("nope"));
        assertFalse(store.del("nope"));
    }

    //region: tests for edge cases

    @Test
    void testEmptyKeyAndValue() {
        store.set("", "", 0);
        assertEquals("", store.get(""));
        assertTrue(store.del(""));
        assertNull(store.get(""));
    }

    @Test
    void testNullKeyOrValueThrows() {
        assertThrows(NullPointerException.class, () -> store.set(null, "v", 0));
        assertThrows(NullPointerException.class, () -> store.set("k", null, 0));
        assertThrows(NullPointerException.class, () -> store.get(null));
        assertThrows(NullPointerException.class, () -> store.del(null));
    }

    @Test
    void testLargeKeyAndValue() {
        String bigKey = "k".repeat(1024);
        String bigVal = "v".repeat(1024 * 10);
        store.set(bigKey, bigVal, 0);
        assertEquals(bigVal, store.get(bigKey));
    }

    @Test
    void testBufferFullThrows() {
        String bigVal = "x".repeat(1024 * 1024); // 1MB, should fill buffer
        assertThrows(RuntimeException.class, () -> store.set("big", bigVal, 0));
    }

    @Test
    void testNegativeTTLMeansNoExpiry() {
        store.set("negttl", "val", -1000);
        assertEquals("val", store.get("negttl"));
    }

    @Test
    void testUnicodeKeyAndValue() {
        String key = "ключ";
        String val = "значение";
        store.set(key, val, 0);
        assertEquals(val, store.get(key));
    }

    @Test
    void testRepeatedDelete() {
        store.set("repeat", "v", 0);
        assertTrue(store.del("repeat"));
        assertFalse(store.del("repeat"));
        assertNull(store.get("repeat"));
    }

    @Test
    void testPersistenceAcrossReopen() throws Exception {
        store.set("persist", "yes", 0);
        store = new StoreEngine(tempFile.getAbsolutePath());
        assertEquals("yes", store.get("persist"));
    }

    //endregion: tests for edge cases

}
