package org.apache.paimon.flink.lookup;

import java.util.concurrent.LinkedBlockingQueue;

public class FixedSizeQueue<E> extends LinkedBlockingQueue<E> {

    private final int maxSize;

    public FixedSizeQueue(int maxSize) {
        super(maxSize);
        this.maxSize = maxSize;
    }

    @Override
    public boolean offer(E e) {
        // If the queue is full, remove the oldest element
        while (size() >= maxSize) {
            poll();
        }
        return super.offer(e);
    }
}
