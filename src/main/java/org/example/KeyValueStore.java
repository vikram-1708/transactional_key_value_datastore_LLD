package org.example;

import java.util.*;

/**
 * Transactional In-Memory Key/Value Store with nested transactions.
 * Supports SET, GET, DELETE, KEYS, BEGIN, COMMIT, ROLLBACK.
 */
public class KeyValueStore<K, V> {
    // Global committed store
    private final Map<K, V> store = new HashMap<>();

    // Stack of nested transactions
    private final Stack<Transaction<K, V>> txStack = new Stack<>();

    // ---------------- Core Operations ----------------
    public void set(K key, V value) {
        if (!txStack.isEmpty()) {
            txStack.peek().getChanges().put(key, value);
        } else {
            store.put(key, value);
        }
    }

    public V get(K key) {
        for (Transaction<K, V> tx : txStack) {
            if (tx.getChanges().containsKey(key)) {
                return tx.getChanges().get(key);
            }
        }
        return store.get(key);
    }

    public void delete(K key) {
        if (!txStack.isEmpty()) {
            txStack.peek().getChanges().remove(key);
        } else {
            store.remove(key);
        }
    }

    public List<K> keys() {
        Set<K> allKeys = new HashSet<>(store.keySet());
        for (Transaction<K, V> tx : txStack) {
            allKeys.addAll(tx.getChanges().keySet());
        }
        return new ArrayList<>(allKeys);
    }

    // ---------------- Transaction Operations ----------------
    public void begin() {
        txStack.push(new Transaction<>());
    }

    public void rollback() {
        if (txStack.isEmpty()) return;
        txStack.pop();
    }

    public void commit() {
        if (txStack.isEmpty()) return;

        Transaction<K, V> top = txStack.pop();

        if (!txStack.isEmpty()) {
            // Merge changes into parent transaction
            txStack.peek().getChanges().putAll(top.getChanges());
        } else {
            // Apply to global store
            store.putAll(top.getChanges());
        }
    }
}
