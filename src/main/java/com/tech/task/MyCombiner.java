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

public final class MyCombiner<T> extends Combiner<T> {
    private final static Logger LOGGER = Logger.getLogger(MyCombiner.class.getName());
    private final CombinerWorker<T> worker;

    private final Object monitor = new Object();
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

    public MyCombiner(SynchronousQueue<T> outputQueue) {
        this(outputQueue, Ordering.allEqual());
    }

    protected MyCombiner(SynchronousQueue<T> outputQueue, Ordering<? super QueueMeta<T>> ordering) {
        super(outputQueue);
        this.worker = new CombinerWorker<T>(this, ordering);
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
        if (meta == null) {
            return false;
        }
        return true;
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
        private final MyCombiner<T> combiner;
        private final Ordering<? super QueueMeta<T>> ordering;

        CombinerWorker(MyCombiner<T> combiner, Ordering<? super QueueMeta<T>> ordering) {
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
                    Object monitor = this.combiner.monitor;
                    synchronized (monitor) {
                        while (this.combiner.queues.isEmpty()) {
                            LOGGER.log(Level.FINE, "Awaiting for a queue to be added");
                            monitor.wait();
                        }
                    }
                } catch (InterruptedException ex) {
                    LOGGER.log(Level.SEVERE, "Interrupted while waiting for a queue to be added, exiting");
                    return;
                }
                // TODO: rather than spin checking everything, derive next event
                // of interest and sleep until then

                // delay parameter basically says about max latency of the queue

                // if we have q with latency = 1s, queue with latency = 2s,
                // queue with latency=60s
                // we can get a lot of elements from the 1st one until 3rd will
                // show something
                // once we get 1st and 2nd having elements but stuffed, we'll
                // try to get something from 60s one
                Collection<QueueMeta<T>> copy = combiner.queues.values();
                if (this.ordering != null) {
                    copy = this.ordering.immutableSortedCopy(copy);
                }
                LOGGER.log(Level.FINE, "Priority sorted list of queues: {0}", copy);

                // TODO: this is an arbitrary timeout and should be changed
                // if any other queue is having a ready value, we should read it
                // if all queues are empty, we can wait for:
                // 1) new being queue added
                // 2) minimal of the time to live values
                // 3) something even smaller as e.g. minimal time can be hour,
                // while queues get something every 1s
                long targetMinLatencyNs = TimeUnit.MILLISECONDS.toNanos(5);
                long deadline = System.nanoTime() + targetMinLatencyNs;
                for (QueueMeta<T> meta : copy) {
                    if (meta.isExpiredAndEmpty()) {
                        LOGGER.log(Level.FINE, "Removing expired and empty queue {0}", meta);
                        combiner.removeInputQueue(meta.queue);
                    }

                    double totalPrioritySum = this.combiner.getTotalPrioritySum();
                    double expectedShare = meta.priority / totalPrioritySum;
                    double totalAfterThisStep = itemsTakenTotal + 1.0;
                    double actualShare = meta.itemsTaken / totalAfterThisStep;

                    // TODO: if we were to have a while loop here, we can get around cases with a queue being slow and low priority
                    LOGGER.log(Level.FINE,
                            "Thinking about reading from queue {0}. " + "Expected share: {1} ({2}/{3}). "
                                    + "Actual share: {4}",
                            new Object[] { meta, expectedShare, meta.priority, totalPrioritySum, actualShare });
                    if (actualShare >= expectedShare) {
                        LOGGER.log(Level.FINE,
                                "Skipping getting items from queue {0} as share calculated ({1}) is larger than expected({2})",
                                new Object[] { meta, actualShare, expectedShare });
                        continue;
                    }

                    long maxWait = Math.max(0, deadline - System.nanoTime());
                    LOGGER.log(Level.FINE, "Waiting up to {1}ns to read from queue {0}",
                            new Object[] { meta, maxWait });
                    T result = meta.tryRead(maxWait, TimeUnit.NANOSECONDS);
                    LOGGER.log(Level.FINE, "Got {0}", result);

                    // TODO: how we remove queues which were timed out while we
                    // were waiting for the consumer to get the value?
                    // having a single thread managing all the meta is kinda fun
                    // in terms of requiring little-to-no sync
                    // having two threads allows to spread the responsibilities
                    if (result == null) {
                        LOGGER.log(Level.FINE, "No item read from queue {0}", meta);
                        continue;
                    }

                    itemsTakenTotal += 1;
                    try {
                        LOGGER.log(Level.FINE, "Putting item to the output queue");
                        this.combiner.outputQueue.put(result);
                        LOGGER.log(Level.FINE, "Succesfully put item to the output queue");
                    } catch (InterruptedException ex) {
                        LOGGER.log(Level.SEVERE, "Problem while putting {0} to the output queue", result);
                    }
                }

                this.spinCount.incrementAndGet();
            }
        }
    }
}