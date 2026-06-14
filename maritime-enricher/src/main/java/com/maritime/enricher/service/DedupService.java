package com.maritime.enricher.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;

/**
 * Deduplication service backed by a Caffeine TTL cache.
 *
 * Deduplicates by (mmsi, timestamp) composite key — the consumer-side
 * idempotency required by at-least-once delivery (Phase 2).
 */
public class DedupService {

    private final Cache<String, Boolean> seenKeys;

    public DedupService() {
        this(Duration.ofHours(1));
    }

    public DedupService(Duration ttl) {
        // Cache entries expire after ttl; maximumSize bounds memory under load.
        this.seenKeys = Caffeine.newBuilder()
                .expireAfterWrite(ttl)
                .maximumSize(1_000_000)
                .build();
    }

    /**
     * Check if a (mmsi, timestamp) key has been seen before.
     * First occurrence returns false (not a duplicate) and caches the key.
     * Subsequent occurrences return true (duplicate).
     */
    public boolean isDuplicate(String mmsi, long timestamp) {
        String key = mmsi + ":" + timestamp;
        // putIfAbsent returns null when the key was absent (first sighting → not a
        // duplicate) and the existing value when the key was already present (duplicate).
        return seenKeys.asMap().putIfAbsent(key, Boolean.TRUE) != null;
    }

    /**
     * Get current cache size (for monitoring/testing).
     */
    public long size() {
        return seenKeys.estimatedSize();
    }
}
