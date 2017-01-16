package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.rx2.Actions;
import com.github.davidmoten.rx2.BiFunctions;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;
import com.github.davidmoten.rx2.Flowables;
import com.github.davidmoten.rx2.Functions;
import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

public class FlowableMatchTest {

    @Test
    public void test() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(2, 1);
        match(a, b, 2, 1);
    }

    @Test
    public void testAsynchronous() {
        Flowable<Integer> a = Flowable.just(1, 2).subscribeOn(Schedulers.computation());
        Flowable<Integer> b = Flowable.just(2, 1);
        match(a, b) //
                .awaitDone(5, TimeUnit.SECONDS) //
                .assertComplete().assertValueSet(Arrays.asList(1, 2));
    }

    @Test
    public void testKeepsRequesting() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(2).repeat(1000).concatWith(Flowable.just(1));
        match(a, b, 1);
    }

    @Test
    public void testKeepsRequestingSwitched() {
        Flowable<Integer> a = Flowable.just(2).repeat(1000).concatWith(Flowable.just(1));
        Flowable<Integer> b = Flowable.just(1);
        match(a, b, 1);
    }

    @Test
    public void test2() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(1, 2);
        match(a, b, 1, 2);
    }

    @Test
    public void test3() {
        Flowable<Integer> a = Flowable.just(1, 2, 3);
        Flowable<Integer> b = Flowable.just(3, 2, 1);
        match(a, b, 3, 2, 1);
    }

    @Test
    public void testOneMatch() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1);
        match(a, b, 1);
    }

    @Test
    public void testEmpties() {
        Flowable<Integer> a = Flowable.empty();
        Flowable<Integer> b = Flowable.empty();
        match(a, b);
    }

    @Test
    public void testRepeats() {
        Flowable<Integer> a = Flowable.just(1, 1);
        Flowable<Integer> b = Flowable.just(1, 1);
        match(a, b, 1, 1);
    }

    @Test
    public void testRepeats2() {
        Flowable<Integer> a = Flowable.just(1, 1, 2, 3, 1);
        Flowable<Integer> b = Flowable.just(1, 2, 1, 3, 1);
        match(a, b, 1, 2, 1, 3, 1);
    }

    @Test
    public void testNoMatchExistsForAtLeastOneFirstLonger() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(1);
        match(a, b, 1);
    }

    @Test
    public void testNoMatchExistsForAtLeastOneFirstLongerSwitched() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1, 2);
        match(a, b, 1);
    }

    @Test
    public void testNoMatchExistsForAtLeastOneSameLength() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(1, 3);
        match(a, b, 1);
    }

    @Test
    public void testNoMatchExistsForAtLeastOneSecondLonger() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1, 2);
        match(a, b, 1);
    }

    @Test
    public void testNoMatchExistsAtAll() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(3, 4);
        match(a, b, new Integer[] {});
    }

    @Test
    public void testBackpressure1() {
        Flowable<Integer> a = Flowable.just(1, 2, 5, 7, 6, 8);
        Flowable<Integer> b = Flowable.just(3, 4, 5, 6, 7);
        matchThem(a, b) //
                .test(0) //
                .assertNoValues()//
                .assertNotTerminated()//
                .requestMore(1)//
                .assertValues(5)//
                .assertNotTerminated()//
                .requestMore(1)//
                .assertValues(5, 6)//
                .assertNotTerminated()//
                .requestMore(1)//
                .assertValues(5, 6, 7)//
                .requestMore(1) //
                .assertValueCount(3) //
                .assertComplete();
    }

    @Test
    public void testUnsubscribe() {
        AtomicBoolean unsubA = new AtomicBoolean(false);
        AtomicBoolean unsubB = new AtomicBoolean(false);
        Flowable<Integer> a = Flowable.just(1, 2, 5, 7, 6, 8).doOnCancel(Actions.setToTrue(unsubA));
        Flowable<Integer> b = Flowable.just(3, 4, 5, 6, 7).doOnCancel(Actions.setToTrue(unsubB));
        final List<Integer> list = new ArrayList<Integer>();
        final AtomicBoolean terminal = new AtomicBoolean();
        matchThem(a, b) //
                .doOnTerminate(Actions.setToTrue(terminal)) //
                .doOnNext(Consumers.println())
                .subscribe(new Subscriber<Integer>() {

                    private Subscription s;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onComplete() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                    }

                    @Override
                    public void onNext(Integer t) {
                        list.add(t);
                        s.cancel();
                    }
                });
        assertFalse(terminal.get());
        assertEquals(Arrays.asList(5), list);
        assertTrue(unsubA.get());
        assertTrue(unsubB.get());
    }

    @Test
    public void testError() {
        RuntimeException e = new RuntimeException();
        Flowable<Integer> a = Flowable.just(1, 2).concatWith(Flowable.<Integer>error(e));
        Flowable<Integer> b = Flowable.just(1, 2, 3);
        match(a, b).assertNoValues().assertError(e);
    }

    @Test
    public void testKeyFunctionAThrowsResultsInErrorEmission() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1);
        Flowables
                .match(a, b, Functions.<Integer, Integer>throwing(), Functions.<Integer>identity(),
                        COMBINER)
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testKeyFunctionBThrowsResultsInErrorEmission() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1);
        Flowables.match(a, b, Functions.identity(), Functions.throwing(), COMBINER).test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testCombinerFunctionBThrowsResultsInErrorEmission() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(2, 1);
        Flowables
                .match(a, b, Functions.identity(), Functions.identity(),
                        BiFunctions.<Integer, Integer, Integer>throwing())
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testCombinerFunctionBThrowsResultsInErrorEmissionSwitched() {
        Flowable<Integer> a = Flowable.just(2, 1);
        Flowable<Integer> b = Flowable.just(1, 2);
        Flowables
                .match(a, b, Functions.identity(), Functions.identity(),
                        BiFunctions.<Integer, Integer, Integer>throwing()) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test(timeout = 5000)
    public void testOneDoesNotCompleteAndOtherMatchedAllShouldFinish() {
        Flowable<Integer> a = Flowable.just(1, 2).concatWith(Flowable.<Integer>never());
        Flowable<Integer> b = Flowable.just(1, 2);
        match(a, b).assertValues(1, 2).assertComplete();
    }

    @Test(timeout = 5000)
    public void testOneDoesNotCompleteAndOtherMatchedAllShouldFinishSwitched() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(1, 2).concatWith(Flowable.<Integer>never());
        match(a, b).assertValues(1, 2).assertComplete();
    }

    @Test
    public void testOneDoesNotCompleteAndOtherMatchedAllShouldFinishSwitched2() {
        Flowable<Integer> a = Flowable.just(1, 2);
        Flowable<Integer> b = Flowable.just(1, 3).concatWith(Flowable.<Integer>never());
        match(a, b).assertValues(1).assertNotTerminated();
    }

    @Test
    public void testLongReversed() {
        for (int n = 1; n < 1000; n++) {
            final int N = n;
            Flowable<Integer> a = Flowable.range(1, n).map(new Function<Integer, Integer>() {
                @Override
                public Integer apply(Integer x) {
                    return N + 1 - x;
                }
            });
            Flowable<Integer> b = Flowable.range(1, n);
            boolean equals = Flowable.sequenceEqual(matchThem(a, b).sorted(), Flowable.range(1, n))
                    .blockingGet();
            assertTrue(equals);
        }
    }

    @Test
    public void testLongShifted() {
        for (int n = 1; n < 1000; n++) {
            testShifted(n, false);
        }
    }

    @Test
    public void testVeryLongShifted() {
        testShifted(1000000, false);
    }

    @Test
    public void testVeryLongShiftedAsync() {
        testShifted(1000000, true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeArgumentThrowsIAE() {
        Flowable<Integer> a = Flowable.just(1);
        Flowable<Integer> b = Flowable.just(1);
        Flowables.match(a, b, Functions.identity(), Functions.identity(), COMBINER, -1);
    }

    private void testShifted(int n, boolean async) {
        Flowable<Integer> a = Flowable.just(0).concatWith(Flowable.range(1, n));
        if (async) {
            a = a.subscribeOn(Schedulers.computation());
        }
        Flowable<Integer> b = Flowable.range(1, n);
        assertTrue(Flowable.sequenceEqual(matchThem(a, b), Flowable.range(1, n)).blockingGet());
    }

    private static Flowable<Integer> matchThem(Flowable<Integer> a, Flowable<Integer> b) {
        return a.compose(
                FlowableTransformers.matchWith(b, Functions.identity(), Functions.identity(), COMBINER));
    }

    private static void match(Flowable<Integer> a, Flowable<Integer> b, Integer... expected) {
        match(a, b).assertValues(expected).assertComplete();
    }

    private static TestSubscriber<Integer> match(Flowable<Integer> a, Flowable<Integer> b) {
        return matchThem(a, b).test();
    }

    private static final BiFunction<Integer, Integer, Integer> COMBINER = new BiFunction<Integer, Integer, Integer>() {
        @Override
        public Integer apply(Integer x, Integer y) {
            return x;
        }
    };

}