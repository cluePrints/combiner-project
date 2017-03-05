package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

class QueueMeta<T> {
    private final static Logger LOGGER = Logger.getLogger(QueueMeta.class.getName());
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
    final int instanceNumber = INSTANCE_COUNT.incrementAndGet();
    final long addedAtNano;
    final BlockingQueue<T> queue;
    final double priority;
    final long isEmptyTimeout;
    final TimeUnit timeUnit;

    // we rely on CombinerWorker being the only thread reading working from this thus do no sync
    long expirationNano;
    long itemsTaken;

    QueueMeta(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) {
        this.queue = queue;
        this.priority = priority;
        this.isEmptyTimeout = isEmptyTimeout;
        this.timeUnit = timeUnit;
        this.addedAtNano = System.nanoTime();
        refreshExpiration();
    }

    T tryRead(long timeout, TimeUnit timeUnit) {
        try {
            T element = this.queue.poll(timeout, timeUnit);
            if (element != null) {
                this.itemsTaken += 1;
                this.refreshExpiration();
            }
            return element;
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "Interrupted while trying to read from input queue");
            return null;
        }
    }

    private void refreshExpiration() {
        this.expirationNano = this.timeUnit.toNanos(this.isEmptyTimeout) + this.addedAtNano;
    }

    boolean isExpiredAndEmpty() {
        return System.nanoTime() > expirationNano && queue.isEmpty();
    }
    
    @Override
    public String toString() {
        return String.format("#%06d", this.instanceNumber);
    }
}