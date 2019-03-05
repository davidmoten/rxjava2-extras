package com.github.davidmoten.rx2.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Functions;
import com.github.davidmoten.rx2.Statistics;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.util.Pair;

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class TransformersTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(Transformers.class);
    }

    @Test
    public void testStatisticsOnEmptyStream() {
        Flowable<Integer> nums = Flowable.empty();
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(0, s.count());
        assertEquals(0, s.sum(), 0.0001);
        assertTrue(Double.isNaN(s.mean()));
        assertTrue(Double.isNaN(s.sd()));
    }

    @Test
    public void testStatisticsOnSingleElement() {
        Flowable<Integer> nums = Flowable.just(1);
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(1, s.count());
        assertEquals(1, s.sum(), 0.0001);
        assertEquals(1.0, s.mean(), 0.00001);
        assertEquals(0, s.sd(), 0.00001);
    }

    @Test
    public void testStatisticsOnMultipleElements() {
        Flowable<Integer> nums = Flowable.just(1, 4, 10, 20);
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(4, s.count());
        assertEquals(35.0, s.sum(), 0.0001);
        assertEquals(8.75, s.mean(), 0.00001);
        assertEquals(7.258615570478987, s.sd(), 0.00001);
        assertEquals(1 + 16 + 100 + 400, s.sumSquares(), 0.0001);
    }

    @Test
    public void testStatisticsPairOnEmptyStream() {
        Flowable<Integer> nums = Flowable.empty();
        boolean isEmpty = nums.compose(Transformers.collectStats(Functions.<Integer>identity())).isEmpty()
                .blockingGet();
        assertTrue(isEmpty);
    }

    @Test
    public void testStatisticsPairOnSingleElement() {
        Flowable<Integer> nums = Flowable.just(1);
        Pair<Integer, Statistics> s = nums.compose(Transformers.collectStats(Functions.<Integer>identity()))
                .blockingLast();
        assertEquals(1, (int) s.a());
        assertEquals(1, s.b().count());
        assertEquals(1, s.b().sum(), 0.0001);
        assertEquals(1.0, s.b().mean(), 0.00001);
        assertEquals(0, s.b().sd(), 0.00001);
    }

    @Test
    public void testStatisticsPairOnMultipleElements() {
        Flowable<Integer> nums = Flowable.just(1, 4, 10, 20);
        Pair<Integer, Statistics> s = nums.compose(Transformers.collectStats(Functions.<Integer>identity()))
                .blockingLast();
        assertEquals(4, s.b().count());
        assertEquals(35.0, s.b().sum(), 0.0001);
        assertEquals(8.75, s.b().mean(), 0.00001);
        assertEquals(7.258615570478987, s.b().sd(), 0.00001);
    }

    @Test
    public void testToString() {
        Statistics s = Statistics.create();
        s = s.add(1).add(2);
        assertEquals("Statistics [count=2, sum=3.0, sumSquares=5.0, mean=1.5, sd=0.5]", s.toString());
    }

    @Test
    public void testInsert() {
        Flowable.just(1, 2) //
                .compose(Transformers.insert(Maybe.just(3))) //
                .test() //
                .assertValues(1, 3, 2, 3) //
                .assertComplete();
    }

    @Test
    public void testInsertWithDelays() {
        TestScheduler s = new TestScheduler();
        TestSubscriber<Integer> ts = //
                Flowable.just(1).delay(1, TimeUnit.SECONDS, s) //
                        .concatWith(Flowable.just(2).delay(3, TimeUnit.SECONDS, s)) //
                        .compose(Transformers.insert(Maybe.just(3).delay(2, TimeUnit.SECONDS, s))) //
                        .test();
        ts.assertNoValues();
        s.advanceTimeBy(1, TimeUnit.SECONDS);
        ts.assertValues(1);
        s.advanceTimeBy(2, TimeUnit.SECONDS);
        ts.assertValues(1, 3);
        s.advanceTimeBy(1, TimeUnit.SECONDS);
        ts.assertValues(1, 3, 2);
        ts.assertComplete();
    }

    @Test
    public void testInsertBackpressure() {
        Flowable.just(1, 2) //
                .compose(Transformers.insert(Maybe.just(3))) //
                .test(0) //
                .assertNoValues() //
                .requestMore(1) //
                .assertValues(1) //
                .requestMore(3) //
                .assertValues(1, 3, 2, 3) //
                .requestMore(1) //
                .assertValueCount(4) //
                .assertComplete();
    }

    @Test
    public void testInsertSourceError() {
        Flowable.<Integer>error(new IOException("boo")) //
                .compose(Transformers.insert(Maybe.just(3))) //
                .test() //
                .assertNoValues() //
                .assertErrorMessage("boo");
    }

    @Test
    public void testInsertError() {
        Flowable.just(1, 2) //
                .compose(Transformers.insert(Maybe.error(new IOException("boo")))) //
                .test() //
                .assertValue(1) //
                .assertErrorMessage("boo");
    }

    @Test
    public void testInsertCancel() {
        TestSubscriber<Integer> ts = Flowable.just(1, 2) //
                .compose(Transformers.insert(Maybe.just(3))) //
                .test(0) //
                .assertNoValues() //
                .requestMore(1) //
                .assertValues(1);
        ts.cancel();
        ts.requestMore(100) //
                .assertValues(1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInsertMapperError() {
        Flowable.just(1, 2) //
                .compose(Transformers.insert(Functions.<Integer, Maybe<Integer>>throwing())) //
                .test() //
                .assertValue(1) //
                .assertError(ThrowingException.class);
    }
    
    @Test
    public void testInsertNothing() {
        Flowable.just(1, 2, 3) //
                .compose(Transformers.insert(Functions.constant(Maybe.<Integer>empty()))) //
                .test() //
                .assertValues(1, 2, 3) //
                .assertComplete();
    }
    
    @Test
    public void testInsertNoOnNextOrCompleteEventsProcessedAfterMapperError() {
        assertEquals(1, countMapperCalls(true));
    }
    
    @Test
    public void testInsertNoOnNextOrErrorEventsProcessedAfterMapperError() {
        assertEquals(1, countMapperCalls(false));
    }

    private int countMapperCalls(final boolean complete) {
        final AtomicInteger count = new AtomicInteger();
        final Function<Integer, Maybe<Integer>> mapper = new Function<Integer, Maybe<Integer>>() {
            @Override
            public Maybe<Integer> apply(Integer t) throws Exception {
                count.incrementAndGet();
                throw new ThrowingException();
            }
        };
        // construct a Flowable that ignores cancellation (and always expects a
        // Long.MAX_VALUE request)
        new Flowable<Integer>() {

            @Override
            protected void subscribeActual(final Subscriber<? super Integer> s) {
                s.onSubscribe(new Subscription() {

                    @Override
                    public void request(long n) {
                        s.onNext(1);
                        s.onNext(2);
                        if (complete) {
                            s.onComplete();
                        } else {
                            s.onError(new IOException("boo"));
                        }
                    }

                    @Override
                    public void cancel() {
                        // ignore
                    }
                });
            }
        } //
                .compose(Transformers.insert(mapper)) //
                .test() //
                .assertValue(1) //
                .assertError(ThrowingException.class);
        return count.get();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBufferMaxCountAndTimeout() {
        int maxSize = 3;
        Flowable.just(1, 2, 3) //
                .compose(Transformers.<Integer>buffer(maxSize, 0, TimeUnit.SECONDS, Schedulers.trampoline())).test() //
                .assertValues(Lists.newArrayList(1), Lists.newArrayList(2), Lists.newArrayList(3)) //
                .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBufferMaxCountAndTimeoutAsyncCountWins() {
        int maxSize = 3;
        Flowable.just(1, 2, 3, 4) //
                .concatWith(Flowable.just(5).delay(500, TimeUnit.MILLISECONDS)) //
                .compose(Transformers.<Integer>buffer(maxSize, 100, TimeUnit.MILLISECONDS)) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertValues(Lists.newArrayList(1, 2, 3), Lists.newArrayList(4), Lists.newArrayList(5)) //
                .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBufferMaxCountAndTimeoutAsyncTimeoutWins() {
        int maxSize = 3;
        Flowable.just(1, 2) //
        .timeout(1, TimeUnit.SECONDS) //
                .concatWith(Flowable.just(3).delay(500, TimeUnit.MILLISECONDS)) //
                .compose(Transformers.<Integer>buffer(maxSize, 100, TimeUnit.MILLISECONDS)) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertValues(Lists.newArrayList(1, 2), Lists.newArrayList(3)) //
                .assertComplete();
    }
    
    @Test
    public void testBufferTimeoutSourceError() {
        Flowable.<Integer>error(new ThrowingException()) //
           .compose(Transformers.<Integer>buffer(3, 100, TimeUnit.MILLISECONDS)) //
           .test() //
           .awaitDone(10, TimeUnit.SECONDS) //
           .assertNoValues() //
           .assertError(ThrowingException.class);
    }
    
    @Test
    public void testInsertTimeoutValueError() {
        Flowable.just(1).concatWith(Flowable.<Integer>never()) //
                .compose(Transformers.<Integer>insert( //
                        Functions.constant(7L), //
                        TimeUnit.MILLISECONDS, //
                        Functions.<Integer, Integer>throwing())) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertValues(1) //
                .assertError(ThrowingException.class);
    }
    
    @Test
    public void testInsertTimeoutCancel() {
        Flowable.just(1, 2) //
                .compose(Transformers.<Integer>insert( //
                        Functions.constant(7L), //
                        TimeUnit.SECONDS, //
                        Functions.<Integer, Integer>throwing())) //
                .take(1) //
                .test() //
                .awaitDone(10, TimeUnit.SECONDS) //
                .assertValue(1);
    }
    
    public static void main(String[] args) {
        Flowable.interval(1, TimeUnit.SECONDS) //
            .compose(Transformers.insert(Maybe.just(-1L).delay(500, TimeUnit.MILLISECONDS))) //
            .doOnNext(Consumers.println()) //
            .count() //
            .blockingGet();
    }
    
}
