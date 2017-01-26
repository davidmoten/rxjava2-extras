package com.github.davidmoten.rx2;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.rx2.internal.SchedulerWithId;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;

public final class SchedulerHelper {

    private SchedulerHelper() {
        // prevent instantiation
    }

    public static Scheduler withThreadIdFromCallSite(Scheduler scheduler) {
        return new SchedulerWithId(scheduler, describeCallSite());
    }

    public static Scheduler withThreadId(Scheduler scheduler, String id) {
        return new SchedulerWithId(scheduler, id);
    }

    private static String describeCallSite() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        StackTraceElement e = elements[3];
        return e.getClassName() + ":" + e.getMethodName() + ":" + e.getLineNumber();
    }

    public static void blockUntilWorkFinished(Scheduler scheduler, int numThreads, long timeout, TimeUnit unit) {
        final CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 1; i <= numThreads; i++) {
            final Worker worker = scheduler.createWorker();
            worker.schedule(new Runnable() {
                @Override
                public void run() {
                    worker.dispose();
                    latch.countDown();
                }
            });
        }
        try {
            boolean finished = latch.await(timeout, unit);
            if (!finished) {
                throw new RuntimeException("timeout occured waiting for work to finish");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void blockUntilWorkFinished(Scheduler scheduler, int numThreads) {
        blockUntilWorkFinished(scheduler, numThreads, 1, TimeUnit.MINUTES);
    }
}
