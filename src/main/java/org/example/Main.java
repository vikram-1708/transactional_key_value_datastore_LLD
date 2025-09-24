package org.example;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        KeyValueStore<String, Object> kv = new KeyValueStore<>();

        // -------- Single-threaded demo --------
        System.out.println("=== Single-threaded demo ===");
        kv.set("a", 10);
        kv.set("b", 20);
        System.out.println("a=" + kv.get("a")); // 10
        System.out.println("b=" + kv.get("b")); // 20

        kv.begin();
        kv.set("a", 30);
        kv.delete("b");
        kv.set("c", 40);
        System.out.println("a=" + kv.get("a")); // 30
        System.out.println("b=" + kv.get("b")); // null (deleted in transaction)
        System.out.println("keys=" + kv.keys()); // [a, c]

        kv.rollback();
        System.out.println("After rollback:");
        System.out.println("a=" + kv.get("a")); // 10
        System.out.println("b=" + kv.get("b")); // 20
        System.out.println("keys=" + kv.keys()); // [a, b]

        kv.begin();
        kv.delete("a");
        kv.commit();
        System.out.println("After commit delete a:");
        System.out.println("a=" + kv.get("a")); // null
        System.out.println("keys=" + kv.keys()); // [b]

        // -------- Multi-threaded demo --------
        System.out.println("\n=== Multi-threaded demo ===");
        KeyValueStore<String, Object> kv2 = new KeyValueStore<>();

        Thread t1 = new Thread(() -> {
            kv2.begin();
            kv2.set("x", 100);
            kv2.set("y", 200);
            System.out.println("[Thread 1] Before commit: x=" + kv2.get("x") + ", y=" + kv2.get("y"));
            kv2.commit();
            System.out.println("[Thread 1] After commit: x=" + kv2.get("x") + ", y=" + kv2.get("y"));
        });

        Thread t2 = new Thread(() -> {
            kv2.begin();
            kv2.set("y", 999);
            kv2.set("z", 300);
            System.out.println("[Thread 2] Before commit: y=" + kv2.get("y") + ", z=" + kv2.get("z"));
            kv2.commit();
            System.out.println("[Thread 2] After commit: y=" + kv2.get("y") + ", z=" + kv2.get("z"));
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        System.out.println("Global store keys: " + kv2.keys()); // should contain x, y, z
        System.out.println("x=" + kv2.get("x") + ", y=" + kv2.get("y") + ", z=" + kv2.get("z"));
    }
}