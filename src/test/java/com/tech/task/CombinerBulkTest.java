package com.tech.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CombinerBulkTest {
    // TODO: also play with: JPF, multithreadedtc, threadweaver
    @Test
    public void testCanCheckQueueExistence() throws Exception {
        LinkedBlockingQueue<String> queue1 = generateQueue(1);
        LinkedBlockingQueue<String> queue2 = generateQueue(2);
        // TODO: adding an empty queue raises the result from 0s to 1s in high priority and 0.5s in super low
        LinkedBlockingQueue<String> queue3 = new LinkedBlockingQueue<>(100);
        SynchronousQueue<String> outputQueue = new SynchronousQueue<>();
        SpinningCombiner<String> combiner = new SpinningCombiner<String>(outputQueue);
        combiner.addInputQueue(queue3, 0.00000001, 1, TimeUnit.SECONDS);
        combiner.addInputQueue(queue2, 0.5, 1, TimeUnit.SECONDS);
        combiner.addInputQueue(queue1, 9.5, 1, TimeUnit.SECONDS);

        List<String> result = new ArrayList<>();
        for (int i=0; i<100; i++) {
            result.add(outputQueue.take());
        }

        int frequency1 = Collections.frequency(result, "queue1");
        int frequency2 = Collections.frequency(result, "queue2");
        Assert.assertEquals(95, frequency1);
        Assert.assertEquals(5, frequency2);
    }

    private LinkedBlockingQueue<String> generateQueue(int num) throws InterruptedException {
        LinkedBlockingQueue<String> queue1 = new LinkedBlockingQueue<>(100);
        for (int i=0; i<100; i++) {
            queue1.put("queue" + num);
        }
        return queue1;
    }
}
