package com.tech.task;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.Ordering;

import net.jcip.annotations.GuardedBy;

/**
 * A combiner using single thread spin checking queues for new arrivals.
 */
public final class SpinningCombiner<T> extends Combiner<T> {
    private final static Logger LOGGER = Logger.getLogger(SpinningCombiner.class.getName());
    private final CombinerWorker<T> worker;

    private final Object monitor = new Object();
    private final long targetMinLatencyNs;
    @GuardedBy("monitor")
    private double totalPrioritySum;
    /**
     * Although `totalPrioritySum` is based on what is in the `queues`, we don't
     * synchronize access to them.
     * 
     * Worst case problem there is incorrect calculation for how much we've
     * satisfied the priorities on average. This should be fixed in the next
     * looping iteration. The assumption here is that we don't add / remove
     * queues too often.
     *
     * If you add and remove queues to the combiner often enough, you may wish
     * to fork the class.
     */
    private final Map<BlockingQueue<T>, QueueMeta<T>> queues = new ConcurrentHashMap<>();

    public SpinningCombiner(SynchronousQueue<T> outputQueue) {
        this(outputQueue, Ordering.allEqual(), 10, TimeUnit.MILLISECONDS);
    }

    /**
     * Lower latency is basically achieved by spinning.
     * If you're not super sensitive, you can save some cpu and logs by adjusting it with the method.
     */
    public static <T> SpinningCombiner<T> withLatency(SynchronousQueue<T> outputQueue, 
            long targetLatency,
            TimeUnit targetLatencyUnit) {
        return new SpinningCombiner<T>(outputQueue, Ordering.allEqual(), 10, TimeUnit.MILLISECONDS);
    }

    protected SpinningCombiner(SynchronousQueue<T> outputQueue,
            Ordering<? super QueueMeta<T>> ordering, 
            long targetLatency,
            TimeUnit targetLatencyUnit) {
        super(outputQueue);
        this.worker = new CombinerWorker<T>(this, ordering);
        this.worker.start();
        this.targetMinLatencyNs = targetLatencyUnit.toNanos(targetLatency);
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
        if (this.queues.containsKey(queue)) {
            throw new IllegalStateException("Queue " + queue + " is already there. Double addition prohibited");
        }

        QueueMeta<T> meta = new QueueMeta<T>(queue, priority, isEmptyTimeout, timeUnit, this.worker);
        this.queues.put(queue, meta);
        synchronized (monitor) {
            this.totalPrioritySum += priority;
            monitor.notifyAll();
        }
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        QueueMeta<T> meta = this.queues.get(queue);
        return meta != null;
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) {
        QueueMeta<T> removed = queues.remove(queue);
        if (removed != null) {
            synchronized (monitor) {
                this.totalPrioritySum -= removed.priority;
            }
        }
    }

    public synchronized double getTotalPrioritySum() {
        return this.totalPrioritySum;
    }

    /**
     * This does some best effort shutdown, intended for testing resource
     * cleanup.
     */
    // visible for testing
    void shutdownUnsafe() throws InterruptedException {
        LOGGER.log(Level.FINE, "Shutting down {0}", this);
        this.worker.setContinueFlag(false);
        this.worker.join(100);
        this.worker.interrupt();
        this.worker.join(100);
        if (this.worker.isAlive()) {
            LOGGER.log(Level.FINE, "Shut down finished for {0}", this);
        } else {
            LOGGER.log(Level.SEVERE, "Worker for {0} is hanging after the shutdown", this);
        }
    }

    // visible for testing
    long getSpinCount() {
        return this.worker.spinCount.get();
    }

    // visible for testing
    void resetSpinCount() {
        LOGGER.log(Level.FINE, "Resetting spin count");
        this.worker.spinCount.set(0);
    }

    private static class CombinerWorker<T> extends Thread {
        private static final AtomicLong instanceCounter = new AtomicLong();
        private final AtomicLong spinCount = new AtomicLong();
        private final AtomicBoolean continueFlag = new AtomicBoolean(true);
        private final SpinningCombiner<T> combiner;
        private final Ordering<? super QueueMeta<T>> ordering;

        CombinerWorker(SpinningCombiner<T> combiner, Ordering<? super QueueMeta<T>> ordering) {
            setName("CombinerThread" + instanceCounter.incrementAndGet());
            setDaemon(true);
            this.combiner = combiner;
            this.ordering = ordering;
            LOGGER.log(Level.FINE, "Initialized worker {0}", getName());
        }

        public void setContinueFlag(boolean flag) {
            this.continueFlag.set(flag);
        }

        @Override
        public void run() {
            long itemsTakenTotal = 0;
            while (continueFlag.get()) {
                try {
                    waitUntilInputQueueWillBeAdded();
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.SEVERE, "Interrupted while waiting for a queue to be added, exiting");
                    return;
                }

                Collection<QueueMeta<T>> copy = getQueues();

                long deadline = System.nanoTime() + this.combiner.targetMinLatencyNs;
                for (QueueMeta<T> meta : copy) {
                    if (meta.isExpiredAndEmpty()) {
                        LOGGER.log(Level.FINE, "Removing expired and empty queue {0}", meta);
                        this.combiner.removeInputQueue(meta.queue);
                        continue;
                    }

                    boolean proceedConsuming = meta.belowTargetFrequency(itemsTakenTotal, this.combiner.totalPrioritySum);
                    if (!proceedConsuming) {
                        continue;
                    }

                    long maxWait = Math.max(0, deadline - System.nanoTime());
                    logWaiting(meta, maxWait);
                    T result = meta.tryRead(maxWait, TimeUnit.NANOSECONDS);
                    LOGGER.log(Level.FINE, "Got {0}", result);

                    if (result == null) {
                        LOGGER.log(Level.FINE, "No item read from queue {0}", meta);
                        continue;
                    }

                    itemsTakenTotal = safePut(itemsTakenTotal, result);
                }

                this.spinCount.incrementAndGet();
            }
        }

        private void waitUntilInputQueueWillBeAdded() throws InterruptedException {
            Object monitor = this.combiner.monitor;
            synchronized (monitor) {
                while (this.combiner.queues.isEmpty()) {
                    LOGGER.log(Level.FINE, "Awaiting for a queue to be added");
                    monitor.wait();
                }
            }
        }

        private Collection<QueueMeta<T>> getQueues() {
            Collection<QueueMeta<T>> copy = combiner.queues.values();
            if (this.ordering != null) {
                copy = this.ordering.immutableSortedCopy(copy);
            }
            LOGGER.log(Level.FINE, "Priority sorted list of queues: {0}", copy);
            return copy;
        }

        private void logWaiting(QueueMeta<T> meta, long maxWait) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "Waiting up to {1}ns to read from queue {0}",
                    new Object[] { meta, maxWait });
            }
        }

        private long safePut(long itemsTakenTotal, T result) {
            itemsTakenTotal += 1;
            try {
                LOGGER.log(Level.FINE, "Putting item to the output queue");
                this.combiner.outputQueue.put(result);
                LOGGER.log(Level.FINE, "Succesfully put item to the output queue");
            } catch (InterruptedException ex) {
                LOGGER.log(Level.SEVERE, "Problem while putting {0} to the output queue", result);
            }
            return itemsTakenTotal;
        }
    }
}