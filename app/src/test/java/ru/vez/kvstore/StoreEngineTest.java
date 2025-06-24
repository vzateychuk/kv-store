package ru.vez.kvstore;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Nested;

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

    @Nested
    class BaseTests {

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

        @Test
        void testExpireUpdatesTTL() throws InterruptedException {
            store.set("foo", "bar", 0);
            assertTrue(store.expire("foo", 100)); // Set TTL to 100ms
            assertEquals("bar", store.get("foo"));
            Thread.sleep(150);
            assertNull(store.get("foo")); // Should be expired
        }

        @Test
        void testExpireOnNonExistentKeyReturnsFalse() {
            assertFalse(store.expire("nope", 1000));
        }

        @Test
        void testExpireWithZeroTTLMeansNoExpiry() throws InterruptedException {
            store.set("foo", "bar", 10);
            assertTrue(store.expire("foo", 0)); // Remove expiry
            Thread.sleep(20);
            assertEquals("bar", store.get("foo"));
        }
    }

    @Nested
    class EdgeCasesTests {

        @Test
        void testEmptyValue() {
            store.set("key", "", 0);
            assertEquals("", store.get("key"));
        }

        @Test
        void testBlankKeyThrows() {
            // get
            assertThrows(IllegalArgumentException.class, () -> store.get(" "));
            assertThrows(IllegalArgumentException.class, () -> store.get(null));
            // set
            assertThrows(IllegalArgumentException.class, () -> store.set(" ", "v", 0));
            assertThrows(IllegalArgumentException.class, () -> store.set(null, "v", 0));
            // del
            assertThrows(IllegalArgumentException.class, () -> store.del(" "));
            assertThrows(IllegalArgumentException.class, () -> store.del(null));
            // expire
            assertThrows(IllegalArgumentException.class, () -> store.expire(" ", 1000));
            assertThrows(IllegalArgumentException.class, () -> store.expire(null, 1000));
        }

        @Test
        void testNullValueThrows() {
            assertThrows(NullPointerException.class, () -> store.set("k", null, 0));
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

        @Test
        void testExpireNegativeTTLThrows() {
            store.set("negttl", "val", 10);
            assertThrows(IllegalArgumentException.class, () -> store.expire("negttl", -1000));
        }

        @Test
        void testExpireNegativeTTLThrowsOnNonExistentKey() {
            assertThrows(IllegalArgumentException.class, () -> store.expire("nope", -1));
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class ConcurrentTests {

        private final  int THREADS = 10;

        private ExecutorService executor;

        @BeforeAll
        void setUpExecutor() {
            executor = Executors.newFixedThreadPool(THREADS); // or any suitable number
        }

        @AfterAll
        void tearDownExecutor() throws InterruptedException {
            executor.shutdown();
            executor.awaitTermination(3, TimeUnit.SECONDS);
        }

        @Test
        void testConcurrentSetAndGet() throws Exception {
            int keysPerThread = 100;
            CountDownLatch latch = new CountDownLatch(THREADS * keysPerThread);

            for (int i = 0; i < THREADS; i++) {
                final int threadNum = i;
                executor.submit(() -> {
                    for (int j = 0; j < keysPerThread; j++) {
                        String key = "key" + threadNum + "_" + j;
                        String val = "val" + threadNum + "_" + j;
                        store.set(key, val, 0);
                        assertEquals(val, store.get(key));
                        latch.countDown();
                    }
                });
            }
            latch.await(5, TimeUnit.SECONDS); // Wait for all threads to finish

            // Verify all keys are set correctly
            for (int i = 0; i < THREADS; i++) {
                for (int j = 0; j < keysPerThread; j++) {
                    String key = "key" + i + "_" + j;
                    String val = "val" + i + "_" + j;
                    assertEquals(val, store.get(key));
                }
            }
        }

        @Test
        void testConcurrentSetAndGetWithSameKey() throws Exception {
            CountDownLatch latch = new CountDownLatch(THREADS);
            String key = "sharedKey";
            String baseValue = "baseValue";
            for (int i = 0; i < THREADS; i++) {
                final int threadNum = i;
                executor.submit(() -> {
                    String value = baseValue + threadNum;
                    store.set(key, value, 0);
                    // Verify that the last set value is what we get
                    assertEquals(value, store.get(key));
                    latch.countDown();
                });
            }            
            latch.await(5, TimeUnit.SECONDS); // Wait for all threads to finish
        }

        @Test
        void testConcurrentSetAndDelete() throws Exception {
            int keysPerThread = 50;

            // Set keys concurrently
            CountDownLatch setLatch = new CountDownLatch(THREADS);
            for (int t = 0; t < THREADS; t++) {
                final int threadNum = t;
                executor.submit(() -> {
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "del" + threadNum + "_" + i;
                        store.set(key, "v" + threadNum + "_" + i, 0);
                    }
                    setLatch.countDown();
                });
            }
            setLatch.await(5, TimeUnit.SECONDS);

            // Delete keys concurrently
            CountDownLatch delLatch = new CountDownLatch(THREADS);
            for (int t = 0; t < THREADS; t++) {
                final int threadNum = t;
                executor.submit(() -> {
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "del" + threadNum + "_" + i;
                        store.del(key);
                    }
                    delLatch.countDown();
                });
            }
            delLatch.await(5, TimeUnit.SECONDS); // Wait for all threads to finish

            // Verify all keys are deleted
            for (int t = 0; t < THREADS; t++) {
                for (int i = 0; i < keysPerThread; i++) {
                    String key = "del" + t + "_" + i;
                    assertNull(store.get(key), key + " should be deleted");
                }
            }
        }

        @Test
        void testConcurrentExpire() throws Exception {
            int keysPerThread = 20;
            CountDownLatch setLatch = new CountDownLatch(THREADS);

            // Set keys
            for (int t = 0; t < THREADS; t++) {
                final int threadNum = t;
                executor.submit(() -> {
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "expire" + threadNum + "_" + i;
                        store.set(key, "v" + threadNum + "_" + i, 0);
                    }
                    setLatch.countDown();
                });
            }
            setLatch.await(5, TimeUnit.SECONDS);

            // Expire keys concurrently
            CountDownLatch expireLatch = new CountDownLatch(THREADS);
            for (int t = 0; t < THREADS; t++) {
                final int threadNum = t;
                executor.submit(() -> {
                    for (int i = 0; i < keysPerThread; i++) {
                        String key = "expire" + threadNum + "_" + i;
                        assertTrue(store.expire(key, 100));
                    }
                    expireLatch.countDown();
                });
            }
            expireLatch.await(5, TimeUnit.SECONDS);

            Thread.sleep(150);

            // All keys should be expired
            for (int t = 0; t < THREADS; t++) {
                for (int i = 0; i < keysPerThread; i++) {
                    String key = "expire" + t + "_" + i;
                    assertNull(store.get(key), key + " should be expired");
                }
            }
        }
    }
}
