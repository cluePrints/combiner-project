package com.tech.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.tech.task.Combiner.CombinerException;

// here I play with latency / throughput numbers
public class CombinerLatencyTest {
    private final static Logger LOGGER = Logger.getLogger(SpinningCombiner.class.getName());

    @BeforeClass
    public static void directLogsToConsole() {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tF %1$tl:%1$tM:%1$tS.%1$tL %4$s [%2$s] %5$s%6$s%n");
        Logger logger = Logger.getLogger("");
        logger.setLevel(Level.ALL);
        StreamHandler handler = new StreamHandler(System.out, new SimpleFormatter());
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
    }
    
    @Ignore
    @Test
    public void testCanCheckQueueExistence() throws Exception {
        SynchronousQueue<Element> outputQueue = new SynchronousQueue<>();
        SpinningCombiner<Element> combiner = new SpinningCombiner<>(outputQueue);
        AtomicBoolean continueFlag = new AtomicBoolean(true);

        int queuesCount = 10;
        ExecutorService executor = startParallelProducers(combiner, continueFlag, queuesCount);

        int timeMs = 1000;
        List<Element> elements = readElements(outputQueue, continueFlag, timeMs);
        executor.shutdown();
        
        Long totalLatencyMs = elements.stream().map(Element::processingTimeMs).reduce((x,y) -> x+y).get();
        LOGGER.log(Level.INFO, "Total latency {0}", totalLatencyMs);
        LOGGER.log(Level.INFO, "Total count {0}", elements.size());

        Map<Long, Long> countsByProducer = elements.stream().map(Element::getProducerNumber).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LOGGER.log(Level.INFO, "Numbers by producer {0}", countsByProducer);
        
        int elementsCount = elements.size();
        Map<Long, Float> shareByProducer = countsByProducer.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() / (float) elementsCount));
        LOGGER.log(Level.INFO, "Share by producer {0}", shareByProducer);
    }

    private List<Element> readElements(
            SynchronousQueue<Element> outputQueue, 
            AtomicBoolean continueFlag,
            int timeMs)
            throws InterruptedException {
        List<Element> elements = new ArrayList<>();
        long endTimeMs = System.currentTimeMillis() + timeMs;

        while (System.currentTimeMillis() < endTimeMs) {
            Element element = outputQueue.take();
            if (element != null) {
                elements.add(element);
                element.markProcessed();
            }
        }
        continueFlag.set(false);
        
        for (int i=0; i<10; i++) {
            Element element = outputQueue.poll(50, TimeUnit.MILLISECONDS);
            if (element != null) {
                elements.add(element);
                element.markProcessed();
            }
        }
        return elements;
    }

    private ExecutorService startParallelProducers(SpinningCombiner<Element> combiner, AtomicBoolean continueFlag, int queuesCount)
            throws CombinerException {
        ExecutorService svc = Executors.newFixedThreadPool(queuesCount);
        int minWaitMs = 5;
        int maxWaitMs = 100;
        for (int i=0; i<queuesCount; i++) {
            Producer producer = new Producer(continueFlag, minWaitMs, maxWaitMs);
            BlockingQueue<Element> dest = producer.getDest();
            svc.submit(producer);
            int priority = 1;
            combiner.addInputQueue(dest, priority, 1, TimeUnit.SECONDS);
        }
        return svc;
    }

    static class Element {
        private final long creationNs = System.nanoTime();
        private final AtomicLong processedAtNs = new AtomicLong();
        private final long producerNumber;
        public Element(long producerNumber) {
            this.producerNumber = producerNumber;
        }
        public void markProcessed() {
            this.processedAtNs.set(System.nanoTime());
        }
        public long processingTimeNs() {
            return this.processedAtNs.get() - this.creationNs;
        }
        public long processingTimeMs() {
            return TimeUnit.NANOSECONDS.toMillis(this.processedAtNs.get() - this.creationNs);
        }
        public long getProducerNumber() {
            return this.producerNumber;
        }
    }
    
    static class Producer implements Runnable {
        private final AtomicBoolean continueFlag;
        private final BlockingQueue<Element> dest = new LinkedBlockingQueue<>();
        private static final AtomicLong instanceCount = new AtomicLong();
        private final long instanceNumber = instanceCount.incrementAndGet();
        private final int minWaitMs;
        private final int maxWaitMs;
        
        public Producer(AtomicBoolean continueFlag, int minWaitMs, int maxWaitMs) {
            this.continueFlag = continueFlag;
            this.minWaitMs = minWaitMs;
            this.maxWaitMs = maxWaitMs;
        }        

        @Override
        public void run() {
            while (this.continueFlag.get()) {
                int delayMs = ThreadLocalRandom.current().nextInt(this.minWaitMs, this.maxWaitMs);
                try {
                    Thread.sleep(delayMs);
                    this.dest.put(new Element(this.instanceNumber));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        
        public BlockingQueue<Element> getDest() {
            return this.dest;
        }
    }
}
