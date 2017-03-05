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

public class MyCombiner<T> extends Combiner<T> {
    private final static Logger LOGGER = Logger.getLogger(MyCombiner.class.getName());
    private final Map<BlockingQueue<T>, QueueMeta<T>> queues = new ConcurrentHashMap<>();
    private final CombinerWorker<T> worker;
    private final Ordering<? super QueueMeta<T>> ordering;
    private double totalPrioritySum;

    public MyCombiner(SynchronousQueue<T> outputQueue) {
        this(outputQueue, Ordering.allEqual());
    }

    protected MyCombiner(SynchronousQueue<T> outputQueue, Ordering<? super QueueMeta<T>> ordering) {
        super(outputQueue);
        this.ordering = ordering;
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
        if (this.queues.containsKey(queue)) {
            throw new IllegalStateException("Queue " + queue + " is already there. Double addition prohibited");
        }

        QueueMeta<T> meta = new QueueMeta<T>(queue, priority, isEmptyTimeout, timeUnit);
        this.queues.put(queue, meta);
        this.totalPrioritySum += priority;
    }

    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        QueueMeta<T> meta = this.queues.get(queue);
        if (meta == null) {
            return false;
        }
        if (meta.isExpiredAndEmpty()) {
            removeInputQueue(queue);
            return false;
        }
        return true;
    }

    @Override
    public void removeInputQueue(BlockingQueue<T> queue) {
        QueueMeta<T> removed = queues.remove(queue);
        if (removed != null) {
            this.totalPrioritySum -= removed.priority;
        }
    }

    // visible for testing
    void shutdownAndJoin() throws InterruptedException {
        LOGGER.log(Level.FINE, "Shutting down {0}", this);
        this.worker.shutdown();
        this.worker.join(5);
        LOGGER.log(Level.FINE, "Shut down finished", this);
    }

    // TODO: how do we clean up?
    // TODO: how do we deal with race condition where an input queue was not
    // empty when we've checked for an element and then it became
    private static class CombinerWorker<T> extends Thread {
        private static final AtomicLong instanceCounter = new AtomicLong();
        private MyCombiner<T> combiner;
        private AtomicBoolean continueFlag = new AtomicBoolean(true);

        CombinerWorker(MyCombiner<T> combiner) {
            setName("CombinerThread" + instanceCounter.incrementAndGet());
            setDaemon(true);
            this.combiner = combiner;
            LOGGER.log(Level.FINE, "Initialized worker {0}", getName());
        }

        public void shutdown() {
            this.continueFlag.set(false);
        }

        @Override
        public void run() {
            long itemsTakenTotal = 0;
            while (continueFlag.get()) {
                // TODO: rather than spin checking everything, derive next event
                // of interest and sleep until then
                Collection<QueueMeta<T>> copy = combiner.queues.values();
                if (this.combiner.ordering != null) {
                    copy = this.combiner.ordering.immutableSortedCopy(copy);
                }
                LOGGER.log(Level.FINE, "Priority sorted list of queues: {0}", copy);
                for (QueueMeta<T> meta : copy) {
                    if (meta.isExpiredAndEmpty()) {
                        LOGGER.log(Level.FINE, "Removing expired and empty queue {0}", meta);
                        combiner.removeInputQueue(meta.queue);
                    }

                    double totalPrioritySum = this.combiner.totalPrioritySum;
                    double expectedShare = meta.priority / totalPrioritySum;
                    double actualShare = meta.itemsTaken / (itemsTakenTotal + 1.0);

                    LOGGER.log(Level.FINE, "Thinking about reading from queue {0}. "
                            + "Expeceted share: {1} ({2}/{3}). "
                            + "Actual share: {4}",
                            new Object[]{ meta, expectedShare, meta.priority, totalPrioritySum, actualShare});
                    if (actualShare >= expectedShare) {
                        LOGGER.log(Level.FINE,
                                "Skipping getting items from queue {0} as share calculated ({1}) is larger than expected({2})",
                                new Object[]{meta, actualShare, expectedShare});
                        continue;
                    }

                    LOGGER.log(Level.FINE, "Trying to read from queue {0}", meta);
                    // TODO: this is arbitrary timeout and should be changed
                    T result = meta.tryRead(5, TimeUnit.MILLISECONDS);
                    LOGGER.log(Level.FINE, "Got an {0}", result);

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
            }
        }
    }
}