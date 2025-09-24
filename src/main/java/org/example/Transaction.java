package org.example;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single transaction.
 * Holds the changes (key -> value) for this transaction.
 */
public class Transaction<K, V> {
    private final Map<K, V> changes;

    public Transaction() {
        this.changes = new HashMap<>();
    }

    public Map<K, V> getChanges() {
        return this.changes;
    }
}
