package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.schedulers.Schedulers;

public class SchedulerHelperTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(SchedulerHelper.class);
    }

    @Test
    public void testWithId() {
        Scheduler s = SchedulerHelper.withThreadId(Schedulers.trampoline(), "boo");
        final StringBuilder b = new StringBuilder();
        String main = Thread.currentThread().getName();
        s.createWorker().schedule(new Runnable() {

            @Override
            public void run() {
                b.append(Thread.currentThread().getName());
            }
        });
        assertEquals(main + "|schedId=[boo]", b.toString());
    }
    
    @Test
    public void testDispose() {
        Scheduler s = SchedulerHelper.withThreadId(Schedulers.trampoline(), "boo");
        Worker w = s.createWorker();
        Assert.assertFalse(w.isDisposed());
        w.dispose();
        Assert.assertTrue(w.isDisposed());
    }

    
    @Test
    public void testWithCallSite() {
        Scheduler s = SchedulerHelper.withThreadIdFromCallSite(Schedulers.trampoline());
        final StringBuilder b = new StringBuilder();
        String main = Thread.currentThread().getName();
        s.createWorker().schedule(new Runnable() {

            @Override
            public void run() {
                b.append(Thread.currentThread().getName());
            }
        });
        System.out.println(b);
        assertEquals(
                main + "|schedId=[com.github.davidmoten.rx2.SchedulerHelperTest:testWithCallSite:48]", b.toString());
    }

}
