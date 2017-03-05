package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CombinerTest {
    private static final int SLEEP_BUFFER = 200;

    @Test
    public void testCanCheckQueueExistence() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);

        Assert.assertFalse(combiner.hasInputQueue(queue1));
        combiner.addInputQueue(queue1, 1.0, 5, TimeUnit.SECONDS);

        Assert.assertTrue(combiner.hasInputQueue(queue1));

        combiner.removeInputQueue(queue1);
        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testRemovingNotExistingQueueIsNoOp() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);

        combiner.removeInputQueue(queue1);
        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testEmptyQueueIsRemovedAfterTimeout() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);

        int timeout = 5;
        combiner.addInputQueue(queue1, 1.0, timeout, TimeUnit.MILLISECONDS);
        Assert.assertTrue(combiner.hasInputQueue(queue1));
        TimeUnit.MILLISECONDS.sleep(200 + timeout);

        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testNotEmptyQueueRemainsEvenAfterTimeout() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        queue1.add("element");
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);

        int timeout = 5;
        combiner.addInputQueue(queue1, 1.0, timeout, TimeUnit.MILLISECONDS);
        Assert.assertTrue(combiner.hasInputQueue(queue1));
        TimeUnit.MILLISECONDS.sleep(SLEEP_BUFFER + timeout);

        Assert.assertTrue(combiner.hasInputQueue(queue1));
    }
}
