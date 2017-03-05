package com.tech.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Ordering;

public class CombinerTest {
    private static final int SLEEP_BUFFER = 200;

    @Rule
    public ExpectedException testRuleException = ExpectedException.none();

    private SynchronousQueue<String> outputQueue;
    private BlockingQueue<String> queue1;
    private BlockingQueue<String> queue2;
    private BlockingQueue<String> queue3;
    private MyCombiner<String> combiner;

    @BeforeClass
    public static void beforeClass() {
        directLogsToConsole();
    }

    @Before
    public void before() {
        queue1 = new LinkedBlockingQueue<String>();
        queue2 = new LinkedBlockingQueue<String>();
        queue3 = new LinkedBlockingQueue<String>();
        outputQueue = new SynchronousQueue<String>();
        Ordering<? super QueueMeta<String>> usingToString = Ordering.usingToString();
        combiner = new MyCombiner<String>(outputQueue, usingToString);
    }

    @After
    public void after() throws Exception {
        combiner.shutdownUnsafe();
    }

    @Test
    public void testCanCheckQueueExistence() throws Exception {
        Assert.assertFalse(combiner.hasInputQueue(queue1));
        combiner.addInputQueue(queue1, 1.0, 5, TimeUnit.SECONDS);

        Assert.assertTrue(combiner.hasInputQueue(queue1));

        combiner.removeInputQueue(queue1);
        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    // race condition with timed removal can make caller's life bad dream
    @Test
    public void testRemovingNotExistingQueueIsNoOp() throws Exception {
        combiner.removeInputQueue(queue1);
        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testAddingQueueTwiceRaisesException() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        this.testRuleException.expect(IllegalStateException.class);
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
    }

    @Test
    public void testEmptyQueueIsRemovedAfterTimeout() throws Exception {
        int timeout = 5;
        combiner.addInputQueue(queue1, 1.0, timeout, TimeUnit.MILLISECONDS);
        Assert.assertTrue(combiner.hasInputQueue(queue1));
        TimeUnit.MILLISECONDS.sleep(200 + timeout);

        Assert.assertFalse(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testNotEmptyQueueRemainsEvenAfterTimeout() throws Exception {
        queue1.add("element");
        queue1.add("element");

        int timeout = 5;
        combiner.addInputQueue(queue1, 1.0, timeout, TimeUnit.MILLISECONDS);
        Assert.assertTrue(combiner.hasInputQueue(queue1));
        TimeUnit.MILLISECONDS.sleep(SLEEP_BUFFER + timeout);

        Assert.assertTrue(combiner.hasInputQueue(queue1));
    }

    @Test
    public void testElementsFlowToOutputQueue() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        queue1.add("element");
        String result = outputQueue.poll(5, TimeUnit.MILLISECONDS);
        Assert.assertEquals("element", result);
    }

    @Test(timeout = 1000)
    public void testUnequalPrioritiesAreRespected() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        combiner.addInputQueue(queue2, 2.0, 1, TimeUnit.HOURS);
        queue1.add("element1");
        queue1.add("element1");
        queue2.add("element2");
        queue2.add("element2");
        TimeUnit.MILLISECONDS.sleep(5);

        List<String> elements = new ArrayList<>();
        elements.add(outputQueue.take());
        elements.add(outputQueue.take());
        elements.add(outputQueue.take());

        int elementCount1 = Collections.frequency(elements, "element1");
        int elementCount2 = Collections.frequency(elements, "element2");
        Assert.assertEquals(1, elementCount1);
        Assert.assertEquals(2, elementCount2);
    }

    @Test(timeout = 1000)
    public void testEqualPrioritiesDontConfuseTheThing() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        combiner.addInputQueue(queue2, 1.0, 1, TimeUnit.HOURS);
        combiner.addInputQueue(queue3, 1.0, 1, TimeUnit.HOURS);
        queue1.add("element1");
        queue1.add("element1");
        queue2.add("element2");
        queue2.add("element2");
        queue3.add("element3");
        queue3.add("element3");
        TimeUnit.MILLISECONDS.sleep(5);

        List<String> elements = new ArrayList<>();
        elements.add(outputQueue.take());
        elements.add(outputQueue.take());
        elements.add(outputQueue.take());

        int elementCount1 = Collections.frequency(elements, "element1");
        int elementCount2 = Collections.frequency(elements, "element2");
        int elementCount3 = Collections.frequency(elements, "element3");
        Assert.assertEquals(1, elementCount1);
        Assert.assertEquals(1, elementCount2);
        Assert.assertEquals(1, elementCount3);
    }

    @Test(timeout = 1000)
    public void testPriorityInversionDoesNotBlockTheThing() throws Exception {
        combiner.addInputQueue(queue1, 9999.0,    1, TimeUnit.HOURS);
        combiner.addInputQueue(queue2, 1.0, 1, TimeUnit.HOURS);
        queue1.add("element2");
        TimeUnit.MILLISECONDS.sleep(5);

        Assert.assertEquals("element2", outputQueue.take());
    }

    @Test
    public void testIfQueueWasRemovedWhileExchangeWasHappeningWeWaitForCompletion() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 1, TimeUnit.HOURS);
        queue1.add("element");
        TimeUnit.MILLISECONDS.sleep(SLEEP_BUFFER);

        combiner.removeInputQueue(queue1);
        Assert.assertTrue(queue1.isEmpty());
        TimeUnit.MILLISECONDS.sleep(SLEEP_BUFFER);

        Assert.assertEquals("element", outputQueue.take());
    }

    @Test
    public void testEmptyCombinerDoesNotSpinTooMuch() throws Exception {
        TimeUnit.MILLISECONDS.sleep(100);
        Assert.assertEquals(0, combiner.getSpinCount());
    }

    @Test(timeout = 1000)
    public void testCombinerStopsSpinningAfterEmptying() throws Exception {
        combiner.addInputQueue(queue1, 1.0, 50, TimeUnit.MILLISECONDS);
        while (combiner.hasInputQueue(queue1)) {
            // wait for the queue removal
        }

        combiner.resetSpinCount();
        TimeUnit.MILLISECONDS.sleep(100);
        Assert.assertThat(combiner.getSpinCount(), Matchers.lessThanOrEqualTo(1L));
    }

    private static void directLogsToConsole() {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tl:%1$tM:%1$tS.%1$tL %4$s [%2$s] %5$s%6$s%n");
        Logger logger = Logger.getLogger("");
        logger.setLevel(Level.ALL);
        StreamHandler handler = new StreamHandler(System.out, new SimpleFormatter());
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
    }
}
