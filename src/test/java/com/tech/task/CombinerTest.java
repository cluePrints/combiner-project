package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CombinerTest {
    @Test
    public void testCanCheckQueueExistence() throws Exception {
        SynchronousQueue<String> outputQueue = new SynchronousQueue<String>();
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<String>();
        MyCombiner<String> combiner = new MyCombiner<String>(outputQueue);

        Assert.assertFalse(combiner.hasInputQueue(queue1));
        combiner.addInputQueue(queue1, 1.0, 5, TimeUnit.SECONDS);
        Assert.assertTrue(combiner.hasInputQueue(queue1));
    }
}
