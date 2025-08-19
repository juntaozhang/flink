package org.apache.flink.streaming.examples.my;

import java.util.concurrent.CompletableFuture;

public class FutureExample {
    public static void main(String[] args) {
        final CompletableFuture<Void> currentFuture = new CompletableFuture<>();
        System.out.println(Thread.currentThread() + " started!");
        currentFuture.thenRun(() -> {
            System.out.println(Thread.currentThread() + " finished!");
        });

        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
            currentFuture.complete(null);
            System.out.println(Thread.currentThread() + " complete!");
        }).start();
    }
}
