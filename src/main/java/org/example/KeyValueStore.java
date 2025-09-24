package org.example;

import java.util.*;

/**
 * Transactional In-Memory Key/Value Store with nested transactions.
 * Supports SET, GET, DELETE, KEYS, BEGIN, COMMIT, ROLLBACK.
 */
public class KeyValueStore<K, V> {
    private final Map<K, V> store;
    private final Stack<Transaction<K, V>> transactionStack;

    public KeyValueStore() {
        this.transactionStack = new Stack<>();
        this.store = new HashMap<>();
    }

    /**
     * Sets the given key to the specified value.
     * If inside an active transaction, the change is stored in the transaction.
     */
    public void set(K key, V value) {
        if (!transactionStack.empty()) {
            transactionStack.peek().getChanges().put(key, value);
        } else {
            store.put(key, value);
        }
    }

    /**
     * Returns the value for the given key.
     * Checks active transactions first, then global store.
     * Returns null if key does not exist.
     */
    public V get(K key) {
        for (Transaction<K, V> txn : transactionStack) {
            if (txn.getChanges().containsKey(key)) {
                return txn.getChanges().get(key);
            }
        }
        return store.get(key);
    }

    /**
     * Deletes the given key.
     * If inside a active transaction, deletion only affects the transaction.
     */
    public void delete(K key) {
        if (!transactionStack.empty()) {
            transactionStack.peek().getChanges().remove(key);
            return;
        }
        store.remove(key);
    }

    /**
     * Returns a list of all unique keys present, including keys in active transactions.
     */
    public List<K> keys() {
        Set<K> keysSet = new HashSet<>(store.keySet());
        for (Transaction<K, V> txn : transactionStack) {
            keysSet.addAll(txn.getChanges().keySet());
        }
        return new ArrayList<>(keysSet);
    }

    // ---------------- Transaction Operations ----------------

    /**
     * Begins a new transaction.
     */
    public void begin() {
        Transaction<K, V> txn = new Transaction<>();
        transactionStack.add(txn);
    }

    /**
     * Rolls back the most recent active transaction.
     * If no transaction is active, does nothing.
     */
    public void rollback() {
        if (!transactionStack.empty()) {
            transactionStack.pop();
        }
    }

    /**
     * Commits the most recent active transaction.
     * If nested, merges into parent transaction.
     * If outermost, applies changes to the global store.
     */
    public void commit() {
        if (!transactionStack.empty()) {
            Transaction<K, V> mostRecentActiveTransaction = transactionStack.pop();

            if (!transactionStack.empty()) {
                transactionStack.peek().getChanges().putAll(mostRecentActiveTransaction.getChanges());
            } else {
                store.putAll(mostRecentActiveTransaction.getChanges());
            }
        }
    }
}
