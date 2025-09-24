package org.example;

import java.util.*;

/**
 * Thread-safe transactional in-memory key/value store with nested transactions.
 *
 * <p>Concurrency Summary:
 * - The global store is a synchronizedMap (thread-safe for individual operations like get, put, remove).
 * - Thread-local transaction stack ensures each thread maintains its own nested transactions, preventing interference between threads.
 */
public class KeyValueStore<K, V> {

    /** Shared global store (thread-safe for individual operations) */
    private final Map<K, V> store = Collections.synchronizedMap(new HashMap<>());

    /** Thread-local stack for nested transactions per thread */
    private final ThreadLocal<Stack<Transaction<K, V>>> transactionStack =
            ThreadLocal.withInitial(Stack::new);

    // ---------------- Core Operations ----------------

    /**
     * Sets the given key to the specified value.
     * If inside a transaction, the change is stored in the thread-local transaction stack.
     *
     * Thread Safety:
     * - Thread-local stack does not require synchronization.
     * - Global store is thread-safe for individual put() operations.
     */
    public void set(K key, V value) {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        if (!stack.isEmpty()) {
            stack.peek().getChanges().put(key, value);
        } else {
            store.put(key, value);
        }
    }

    /**
     * Returns the value for the given key.
     * Checks active transactions first, then the global store.
     *
     * Thread Safety:
     * - Reads from thread-local stack are inherently safe.
     * - Reads from the synchronized global store are thread-safe.
     */
    public V get(K key) {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        for (int i = stack.size() - 1; i >= 0; i--) {
            Transaction<K, V> txn = stack.get(i);
            if (txn.getChanges().containsKey(key)) {
                return txn.getChanges().get(key);
            }
        }
        return store.get(key);
    }

    /**
     * Deletes the given key.
     * If inside a transaction, deletion only affects the current transaction.
     * Else delete from global store.
     *
     * Thread Safety:
     * - Thread-local stack requires no synchronization.
     * - Global store is thread-safe for individual remove() operations.
     */
    public void delete(K key) {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        if (!stack.isEmpty()) {
            stack.peek().getChanges().put(key, null);
        } else {
            store.remove(key);
        }
    }

    /**
     * Returns a list of all unique keys, including thread-local transaction changes.
     *
     * Thread Safety:
     * - Accessing the global store is synchronized to create a consistent snapshot.
     * - Thread-local changes are inherently safe.
     */
    public List<K> keys() {
        Set<K> keysSet;
        synchronized (store) {
            keysSet = new HashSet<>(store.keySet());
        }
        Stack<Transaction<K, V>> stack = transactionStack.get();
        for (Transaction<K, V> txn : stack) {
            keysSet.addAll(txn.getChanges().keySet());
        }
        return new ArrayList<>(keysSet);
    }

    // ---------------- Transaction Operations ----------------

    /**
     * Begins a new transaction for the current thread.
     *
     * Thread Safety:
     * - Transaction stack is thread-local, no synchronization needed.
     */
    public void begin() {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        stack.push(new Transaction<>());
    }

    /**
     * Rolls back the most recent active transaction for the current thread.
     *
     * Thread Safety:
     * - Transaction stack is thread-local, no synchronization needed.
     */
    public void rollback() {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        if (!stack.isEmpty()) stack.pop();
    }

    /**
     * Commits the most recent active transaction for the current thread.
     * If nested, merges into parent transaction. If outermost, applies changes to the global store.
     *
     * Thread Safety:
     * - Thread-local stack operations are safe.
     * - Global store putAll() is synchronized to ensure the entire transaction commit happens atomically,
     *   preventing interleaving of updates from multiple threads.
     */
    public void commit() {
        Stack<Transaction<K, V>> stack = transactionStack.get();
        if (stack.isEmpty()) return;

        Transaction<K, V> top = stack.pop();

        if (!stack.isEmpty()) {
            // Merge into parent transaction
            stack.peek().getChanges().putAll(top.getChanges());
        } else {
            // Apply to global store atomically
            synchronized (store) {
                store.putAll(top.getChanges());
            }
        }
    }
}
