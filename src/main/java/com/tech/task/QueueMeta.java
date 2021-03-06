package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

class QueueMeta<T> {
    private final static Logger LOGGER = Logger.getLogger(QueueMeta.class.getName());
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
    private final int instanceNumber = INSTANCE_COUNT.incrementAndGet();
    private final long addedAtNano;
    private final BlockingQueue<T> queue;
    private final double priority;
    private final long isEmptyTimeout;
    private final TimeUnit timeUnit;
    private final Thread owner;

    // we rely on CombinerWorker being the only thread reading working from this
    // thus do no sync
    private long expirationNano;
    private long itemsTaken;

    QueueMeta(BlockingQueue<T> queue,
            double priority,
            long isEmptyTimeout,
            TimeUnit timeUnit,
            Thread owner) {
        this.queue = queue;
        this.priority = priority;
        this.isEmptyTimeout = isEmptyTimeout;
        this.timeUnit = timeUnit;
        this.addedAtNano = System.nanoTime();
        this.owner = owner;
        refreshExpiration0();
    }

    public double getPriority() {
        return this.priority;
    }

    public BlockingQueue<T> getQueue() {
        return this.queue;
    }

    T tryRead(long timeout, TimeUnit timeUnit) {
        checkCallingThread();
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

    boolean belowTargetFrequency(long itemsTakenTotal, double totalPrioritySum) {
        double expectedShare = this.priority / totalPrioritySum;
        double totalAfterThisStep = itemsTakenTotal + 1.0;
        double actualShare = this.itemsTaken / totalAfterThisStep;

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE,
                    "Thinking about reading from queue {0}. " + "Expected share: {1} ({2}/{3}). " + "Actual share: {4}",
                    new Object[] { this, expectedShare, this.priority, totalPrioritySum, actualShare });
        }
        return actualShare < expectedShare;
    }

    private void refreshExpiration() {
        checkCallingThread();
        refreshExpiration0();
    }

    private void checkCallingThread() {
        Preconditions.checkState(Thread.currentThread() == owner);
    }

    private void refreshExpiration0() {
        this.expirationNano = this.timeUnit.toNanos(this.isEmptyTimeout) + this.addedAtNano;
    }

    boolean isExpiredAndEmpty() {
        checkCallingThread();
        return System.nanoTime() > expirationNano && queue.isEmpty();
    }

    @Override
    public String toString() {
        return String.format("#%06d", this.instanceNumber);
    }
}
