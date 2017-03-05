package com.tech.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    @Test
    public void testElementsFlowToOutputQueue() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        queue1.add("element");
        String result = outputQueue.poll(5, TimeUnit.MILLISECONDS);
        Assert.assertEquals("element", result);
    }

    @Test
    public void testPrioritiesAreRespected() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        BlockingQueue<String> queue2 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        combiner.addInputQueue(queue2, 2.0, 1, TimeUnit.HOURS);
        queue1.add("element1");
        queue1.add("element1");
        queue2.add("element2");
        queue2.add("element2");

        List<String> elements = new ArrayList<>();
        int number = outputQueue.drainTo(elements, 3);
        Assert.assertEquals(3, number);
        int elementCount1 = Collections.frequency(elements, "element1");
        int elementCount2 = Collections.frequency(elements, "element2");
        Assert.assertEquals(1, elementCount1);
        Assert.assertEquals(2, elementCount2);
    }
}
