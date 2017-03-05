package com.tech.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MyCombiner<T> extends Combiner<T> {
    private final PriorityQueue<QueueMeta<T>> heap = new PriorityQueue<>();
    private final Map<BlockingQueue<T>, QueueMeta<T>> queues = new HashMap<>();
    private final CombinerWorker<T> worker;

    protected MyCombiner(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
        this.worker = new CombinerWorker<T>(this);
        this.worker.start();
    }

    @Override
    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit)
            throws com.tech.task.Combiner.CombinerException {
        if (priority <= 0) {
            throw new IllegalArgumentException("Priority should be greater than zero. Was " + priority);
        }
        if (isEmptyTimeout <= 0) {
            throw new IllegalArgumentException(
                    "Timeout should be greater than zero. Was " + isEmptyTimeout + " " + timeUnit);
        }

        QueueMeta<T> meta = new QueueMeta<T>(queue, priority, isEmptyTimeout, timeUnit);
        // TODO: this is not too safe, add a concurrency test
        this.heap.add(meta);
        this.queues.put(queue, meta);
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        return this.queues.containsKey(queue);
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) {
        QueueMeta<T> meta = queues.remove(queue);
        this.heap.remove(meta);
    }

    // visible for testing
    void shutdownAndJoin() throws InterruptedException {
        this.worker.shutdown();
        this.worker.join();
    }
    // TODO: how do we clean up?
    // TODO: how do we deal with race condition where an input queue was not
    // empty when we've checked for an element and then it became
    // TODO: thread to have daemon = true

    private static class CombinerWorker<T> extends Thread {
        private static final AtomicLong instanceCounter = new AtomicLong();
        private MyCombiner<T> combiner;
        private AtomicBoolean continueFlag = new AtomicBoolean(true);

        CombinerWorker(MyCombiner<T> combiner) {
            setName("CombinerThread" + instanceCounter.incrementAndGet());
            setDaemon(true);
            this.combiner = combiner;
        }

        public void shutdown() {
            this.continueFlag.set(false);
        }

        @Override
        public void run() {
            while (continueFlag.get()) {
                // TODO: rather than spin checking everything, derive next event
                // of interest and sleep until then
                Collection<QueueMeta<T>> copy = new ArrayList<QueueMeta<T>>(combiner.heap);
                for (QueueMeta<T> meta : copy) {
                    if (meta.isExpiredAndEmpty()) {
                        combiner.removeInputQueue(meta.queue);
                    }

                    T result = meta.tryRead();
                    // TODO: what do we do if a queue was removed while we were
                    // waiting for the consumer to accept the thing?
                    // TODO: how we remove queues which were timed out while we
                    // were waiting for the consumer to get the value?
                    // having a single thread managing all the meta is kinda fun
                    // in terms of requiring little-to-no sync
                    // having two threads allows to spread the responsibilities
                    if (result == null) {
                        continue;
                    }

                    try {
                        this.combiner.outputQueue.put(result);
                    } catch (InterruptedException ex) {
                        // TODO: log instead
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    private static class QueueMeta<T> {
        final long addedAtNano;
        long expirationNano;
        final BlockingQueue<T> queue;
        final double priority;
        final long isEmptyTimeout;
        final TimeUnit timeUnit;

        QueueMeta(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit) {
            super();
            this.queue = queue;
            this.priority = priority;
            this.isEmptyTimeout = isEmptyTimeout;
            this.timeUnit = timeUnit;
            this.addedAtNano = System.nanoTime();
            refreshExpiration();
        }

        private T tryRead() {
            try {
                T element = this.queue.poll(5, TimeUnit.MILLISECONDS);
                if (element != null) {
                    this.refreshExpiration();
                }
                return element;
            } catch (InterruptedException ex) {
                // TODO: use logging instead to have a thread name captured etc
                ex.printStackTrace();
                return null;
            }
        }

        private void refreshExpiration() {
            this.expirationNano = this.timeUnit.toNanos(this.isEmptyTimeout) + this.addedAtNano;
        }

        boolean isExpiredAndEmpty() {
            return System.nanoTime() > expirationNano && queue.isEmpty();
        }

    }
}