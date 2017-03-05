package com.tech.task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class MyCombiner<T> extends Combiner<T>{
    protected MyCombiner(SynchronousQueue<T> outputQueue) {
        super(outputQueue);
    }

    @Override
    public void addInputQueue(BlockingQueue<T> queue, double priority, long isEmptyTimeout, TimeUnit timeUnit)
            throws com.tech.task.Combiner.CombinerException {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public boolean hasInputQueue(BlockingQueue<T> queue) {
        // TODO Auto-generated method stub
        return false;
    }
    
    @Override
    public void removeInputQueue(BlockingQueue<T> queue) throws com.tech.task.Combiner.CombinerException {
        // TODO Auto-generated method stub
        
    }
    
}