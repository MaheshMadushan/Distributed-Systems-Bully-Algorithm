package org.uom.distributed.systems.Utilities;

import org.uom.distributed.systems.Config;

import java.util.concurrent.atomic.AtomicInteger;

public class Timer {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int count;

    public Timer(int count) {
        this.count = count;
        this.counter.set(count);
    }

    public void start() {
        while (counter.get() >= 0) {
            counter.addAndGet(-1);
            try {
                Thread.sleep(Config.UNIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void reset() {
        this.counter.set(this.count);
    }
}
