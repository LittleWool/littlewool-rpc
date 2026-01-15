package com.littlewool.tech.insight.rpc.breaker;

import com.littlewool.tech.insight.rpc.metrics.RpcCallMetrics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @ClassName: ResponseTimeCircuitBreaker
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/15 18:05
 * @Version: 1.0
 **/

public class ResponseTimeCircuitBreaker implements CirCuitBreaker {
    private final AtomicReference<State> stateReference = new AtomicReference<>(State.CLOSE);

    private final long breakMs = 5000L;


    private final long windowDuration = 10000L;

    private final long slotMs = 1000L;

    private final long slowRequestMs;

    private final double slowRatio;

    private final int minRequest = 5;

    private final Slot[] slots = new Slot[(int) (windowDuration / slotMs)];

    private final Lock slideLock = new ReentrantLock();

    private volatile int currentIndex = 0;

    private volatile long breakStartTime = 0L;

    private volatile long currentTime = System.currentTimeMillis() / slotMs * slotMs;


    public ResponseTimeCircuitBreaker(double slowRatio, long slowRequestMs) {
        this.slowRatio = slowRatio;
        this.slowRequestMs = slowRequestMs;
        Arrays.fill(slots, new Slot());
    }

    @Override
    public boolean allowRequest() {
        if (stateReference.get() == State.CLOSE) {
            return true;
        }
        if (stateReference.get() == State.HALF_OPEN) {
            return false;
        }
        if (System.currentTimeMillis() - breakStartTime < breakMs) {
            return false;
        }
        return stateReference.compareAndSet(State.OPEN, State.HALF_OPEN);

    }

    @Override
    public void recordRpc(RpcCallMetrics metrics) {
        long now = System.currentTimeMillis();
        slideWindowIfNecessary(now);

        boolean slowRequest = !metrics.isComplete() || metrics.getDuration() > slowRequestMs;
        switch (stateReference.get()) {
            case OPEN -> processOpen(slowRequest);
            case CLOSE -> processClose(slowRequest);
            case HALF_OPEN -> processHalfOpen(slowRequest);
        }
    }

    private void processClose(boolean slowRequest) {
        if (!slowRequest) {
            slots[currentIndex].requestCount.incrementAndGet();
            return;
        } else {
            slots[currentIndex].requestCount.incrementAndGet();
            slots[currentIndex].errorRequestCount.incrementAndGet();
            int totalRequest = 0;
            int totalErrorRequest = 0;
            for (int i = 0; i < slots.length; i++) {
                totalRequest += slots[i].requestCount.get();
                totalErrorRequest += slots[i].errorRequestCount.get();
            }
            if (totalRequest >= minRequest && ((double) totalErrorRequest) / totalRequest >= slowRatio) {
                if (stateReference.compareAndSet(State.CLOSE, State.OPEN)) {
                    this.breakStartTime = System.currentTimeMillis();
                }
            }
            return;
        }
    }

    private void processHalfOpen(boolean slowRequest) {
        if (!slowRequest) {
            this.stateReference.compareAndSet(State.HALF_OPEN, State.CLOSE);
            return;
        }
        if (this.stateReference.compareAndSet(State.HALF_OPEN, State.OPEN)) {
            this.breakStartTime = System.currentTimeMillis();
        }
    }

    private void processOpen(boolean slowRequest) {

    }

    private void slideWindowIfNecessary(long now) {
        if (now - currentIndex < slotMs) {
            return;
        }
        try {
            slideLock.lock();
            int diff = (int) ((now - currentIndex) / slotMs);
            if (diff <= 0) {
                //双重检查锁
                return;
            }
            int step = Math.min(diff, slots.length);
            for (int i = 0; i < step; i++) {
                int updateIndex = (currentIndex + i + 1) % slots.length;
                Slot slot = slots[updateIndex];
                slot.requestCount.set(0);
                slot.errorRequestCount.set(0);
            }
            currentIndex = (currentIndex + diff) % slots.length;
            currentTime = now / slotMs * slotMs;
        } finally {
            slideLock.unlock();
        }
    }

    private class Slot {
        AtomicInteger requestCount = new AtomicInteger(0);
        AtomicInteger errorRequestCount = new AtomicInteger(0);
    }
}
