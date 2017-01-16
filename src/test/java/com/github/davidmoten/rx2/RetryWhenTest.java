package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.RetryWhen.ErrorAndDuration;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class RetryWhenTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(RetryWhen.class);
    }

    @Test
    public void testExponentialBackoff() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        final AtomicInteger logCalls = new AtomicInteger();
        Consumer<ErrorAndDuration> log = new Consumer<ErrorAndDuration>() {

            @Override
            public void accept(ErrorAndDuration e) {
                System.out.println("WARN: " + e.throwable().getMessage());
                System.out.println("waiting for " + e.durationMs() + "ms");
                logCalls.incrementAndGet();
            }
        };
        Flowable.just(1, 2, 3)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(RetryWhen.maxRetries(5).action(log)
                        .exponentialBackoff(10, TimeUnit.MILLISECONDS).build())
                // go
                .subscribe(ts);

        // check results
        ts.awaitTerminalEvent();
        ts.assertError(ex);
        ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3);
        assertEquals(5, logCalls.get());
    }

    @Test
    public void testWithScheduler() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(RetryWhen.maxRetries(2).action(log)
                        .exponentialBackoff(1, TimeUnit.MINUTES).scheduler(scheduler).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertNotComplete();
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValues(1, 2, 1, 2);
        ts.assertNotComplete();
        // next wait is 2 seconds so advancing by 1 should do nothing
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValues(1, 2, 1, 2);
        ts.assertNotComplete();
        scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.assertValues(1, 2, 1, 2, 1, 2);
        ts.assertError(ex);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRetryWhenSpecificExceptionFails() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(RetryWhen.maxRetries(2).action(log)
                        .exponentialBackoff(1, TimeUnit.MINUTES).scheduler(scheduler)
                        .failWhenInstanceOf(IllegalArgumentException.class).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertError(ex);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRetryWhenSpecificExceptionFailsBecauseIsNotInstanceOf() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(RetryWhen.maxRetries(2).action(log)
                        .exponentialBackoff(1, TimeUnit.MINUTES).scheduler(scheduler)
                        .retryWhenInstanceOf(SQLException.class).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertError(ex);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRetryWhenSpecificExceptionAllowed() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(RetryWhen.maxRetries(2).action(log)
                        .exponentialBackoff(1, TimeUnit.MINUTES).scheduler(scheduler)
                        .retryWhenInstanceOf(IllegalArgumentException.class).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertNotComplete();
    }

    private static final Consumer<ErrorAndDuration> log = new Consumer<ErrorAndDuration>() {

        @Override
        public void accept(ErrorAndDuration e) {
            System.out.println("WARN: " + e.throwable().getMessage());
            System.out.println("waiting for " + e.durationMs() + "ms");
        }
    };

    @Test
    public void testRetryWhenSpecificExceptionAllowedUsePredicateReturnsTrue() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Predicate<Throwable> predicate = new Predicate<Throwable>() {
            @Override
            public boolean test(Throwable t) {
                return t instanceof IllegalArgumentException;
            }
        };
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(
                        RetryWhen.maxRetries(2).action(log).exponentialBackoff(1, TimeUnit.MINUTES)
                                .scheduler(scheduler).retryIf(predicate).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertNotComplete();
    }

    @Test
    public void testRetryWhenSpecificExceptionAllowedUsePredicateReturnsFalse() {
        Exception ex = new IllegalArgumentException("boo");
        TestSubscriber<Integer> ts = TestSubscriber.create();
        TestScheduler scheduler = new TestScheduler();
        Predicate<Throwable> predicate = Predicates.alwaysFalse();
        Flowable.just(1, 2)
                // force error after 3 emissions
                .concatWith(Flowable.<Integer>error(ex))
                // retry with backoff
                .retryWhen(
                        RetryWhen.maxRetries(2).action(log).exponentialBackoff(1, TimeUnit.MINUTES)
                                .scheduler(scheduler).retryIf(predicate).build())
                // go
                .subscribe(ts);
        ts.assertValues(1, 2);
        ts.assertError(ex);
    }

    @Test
    public void testRetryWhenMultipleRetriesWorkOnSingleDelay() {
        AtomicInteger count = new AtomicInteger();
        TestSubscriber<Object> ts = TestSubscriber.create();
        Exception exception = new Exception("boo");
        Flowable.error(exception) //
                .doOnSubscribe(Consumers.increment(count)) //
                .retryWhen(RetryWhen //
                        .delay(1, TimeUnit.MILLISECONDS) //
                        .scheduler(Schedulers.trampoline()) //
                        .maxRetries(10).build()) //
                .subscribe(ts);
        ts.assertTerminated();
        assertFalse(ts.errors().isEmpty());
        assertEquals(exception, ts.errors().get(0));
        assertEquals(11, count.get());
    }

}
