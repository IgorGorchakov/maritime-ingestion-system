package com.maritime.streaming.service;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DedupService}.
 *
 * Tests the at-least-once idempotency contract: a (mmsi, timestamp) composite key
 * must be idempotent — first sighting returns false, every subsequent sighting
 * returns true until the TTL expires.
 */
class DedupServiceTest {

    private final DedupService dedup = new DedupService(Duration.ofMinutes(60));

    @Test
    void firstSighting_notDuplicate() {
        assertThat(dedup.isDuplicate("123456789", 1_700_000_000_000L)).isFalse();
    }

    @Test
    void secondSighting_sameKey_isDuplicate() {
        long ts = 1_700_000_001_000L;
        dedup.isDuplicate("123456789", ts);  // prime
        assertThat(dedup.isDuplicate("123456789", ts)).isTrue();
    }

    @Test
    void differentTimestamp_sameMMSI_notDuplicate() {
        dedup.isDuplicate("123456789", 1_700_000_000_000L);
        // A different timestamp = different report, even for the same vessel.
        assertThat(dedup.isDuplicate("123456789", 1_700_000_001_000L)).isFalse();
    }

    @Test
    void differentMMSI_sameTimestamp_notDuplicate() {
        dedup.isDuplicate("123456789", 1_700_000_000_000L);
        assertThat(dedup.isDuplicate("987654321", 1_700_000_000_000L)).isFalse();
    }

    @Test
    void shortTtl_expiresAndAllowsReplay() throws InterruptedException {
        DedupService shortTtl = new DedupService(Duration.ofMillis(100));
        shortTtl.isDuplicate("111222333", 42L);         // prime
        assertThat(shortTtl.isDuplicate("111222333", 42L)).isTrue();  // still cached
        Thread.sleep(200);                               // let TTL expire
        // After expiry the key is gone — same (mmsi, ts) is treated as fresh
        assertThat(shortTtl.isDuplicate("111222333", 42L)).isFalse();
    }

    @Test
    void concurrentInserts_onlyFirstReturnsFalse() throws Exception {
        int threads = 20;
        long ts = 1_700_000_002_000L;
        CountDownLatch ready  = new CountDownLatch(threads);
        CountDownLatch start  = new CountDownLatch(1);
        CopyOnWriteArrayList<Boolean> results = new CopyOnWriteArrayList<>();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                results.add(dedup.isDuplicate("999888777", ts));
            });
        }
        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Exactly one thread must have seen false (first insert); all others must see true.
        long nonDuplicates = results.stream().filter(b -> !b).count();
        assertThat(nonDuplicates).isEqualTo(1);
    }
}
