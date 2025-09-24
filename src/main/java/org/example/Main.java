package org.example;

public class Main {
    public static void main(String[] args) {
        KeyValueStore<String, Object> kv = new KeyValueStore<>();

        kv.set("a", 10);
        kv.set("b", 20);
        System.out.println("a=" + kv.get("a")); // 10

        kv.begin();
        kv.set("a", 30);
        kv.delete("b");
        kv.set("c", 40);
        System.out.println("a=" + kv.get("a")); // 30
        System.out.println("b=" + kv.get("b")); // null
        System.out.println("keys=" + kv.keys()); // [a, c]

        kv.rollback();
        System.out.println("a=" + kv.get("a")); // 10
        System.out.println("b=" + kv.get("b")); // 20
        System.out.println("keys=" + kv.keys()); // [a, b]

        kv.begin();
        kv.delete("a");
        kv.commit();
        System.out.println("a=" + kv.get("a")); // null
        System.out.println("keys=" + kv.keys()); // [b]
    }
}