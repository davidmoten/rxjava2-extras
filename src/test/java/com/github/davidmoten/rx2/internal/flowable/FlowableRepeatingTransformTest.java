package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.davidmoten.rx2.Actions;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;
import io.reactivex.Notification;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class FlowableRepeatingTransformTest {

    private static final Function<List<Integer>, Integer> sum = (new Function<List<Integer>, Integer>() {
        @Override
        public Integer apply(List<Integer> list) throws Exception {
            int sum = 0;
            for (int value : list) {
                sum += value;
            }
            return sum;
        }
    });

    private static final Function<Flowable<Integer>, Flowable<Integer>> reducer = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            return f.buffer(2).map(sum);
        }
    };

    private static final Function<Flowable<Integer>, Flowable<Integer>> plusOne = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            return f.map(new Function<Integer, Integer>() {

                @Override
                public Integer apply(Integer t) throws Exception {
                    return t + 1;
                }
            });
        }
    };

    private static final Function<Flowable<Integer>, Flowable<Integer>> reducerThrows = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            throw new ThrowingException();
        }
    };

    private static final Function<Flowable<Integer>, Flowable<Integer>> reducerThrowsOnThird = new Function<Flowable<Integer>, Flowable<Integer>>() {
        final AtomicInteger count = new AtomicInteger();

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            if (count.incrementAndGet() >= 3) {
                throw new ThrowingException();
            } else {
                return reducer.apply(f);
            }
        }
    };

    private static final Function<Flowable<Integer>, Flowable<Integer>> reducerAsync = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            return f.subscribeOn(Schedulers.computation()).buffer(2).map(sum);
        }
    };

    @Test
    public void testEmpty() {
        int result = Flowable.<Integer>empty() //
                .to(Transformers.reduce(reducer, 2)) //
                .single(-1) //
                .blockingGet();
        Assert.assertEquals(-1, result);
    }

    @Test
    public void testOne() {
        check(1, 2);
    }

    @Test
    public void testOneAsync() {
        checkAsync(1, 2);
    }

    @Test
    public void testCompletesFirstLevel() {
        check(2, 2);
    }

    @Test
    public void testCompletesSecondLevel() {
        check(3, 2);
    }

    @Test(timeout = 1000)
    public void testCompletesThirdLevel() {
        check(4, 2);
    }

    @Test
    public void testCompletesThirdLevelWithOneLeftOver() {
        check(5, 2);
    }

    @Test
    public void testCompletesFourLevels() {
        check(8, 2);
    }

    @Test
    public void testMany() {
        for (int n = 5; n <= 100; n++) {
            int m = (int) Math.round(Math.floor(Math.log(n) / Math.log(2))) - 1;
            for (int maxChained = Math.max(3, m); maxChained < 6; maxChained++) {
                System.out.println("maxChained=" + maxChained + ",n=" + n);
                check(n, maxChained);
            }
        }
    }

    @Test
    public void testManyAsync() {
        for (int n = 5; n <= 100; n++) {
            int m = (int) Math.round(Math.floor(Math.log(n) / Math.log(2))) - 1;
            for (int maxChained = Math.max(3, m); maxChained < 6; maxChained++) {
                System.out.println("maxChained=" + maxChained + ",n=" + n);
                checkAsync(n, maxChained);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxChainedGreaterThanZero() {
        check(10, 0);
    }

    @Test
    public void testReducerThrows() {
        Flowable.range(1, 10) //
                .to(Transformers.reduce(reducerThrows, 2)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testReducerThrowsOnThirdCall() {
        Flowable.range(1, 128) //
                .to(Transformers.reduce(reducerThrowsOnThird, 2)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testUpstreamCancelled() {
        AtomicBoolean cancelled = new AtomicBoolean();
        Flowable.<Integer>never() //
                .doOnCancel(Actions.setToTrue(cancelled)) //
                .to(Transformers.reduce(reducer, 2)) //
                .test().cancel();
        assertTrue(cancelled.get());
    }

    @Test
    public void testErrorPreChaining() {
        Flowable.<Integer>error(new ThrowingException()) //
                .to(Transformers.reduce(reducer, 2)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    @Ignore
    public void testErrorPreChainingCausesCancel() {
        AtomicBoolean cancelled = new AtomicBoolean();
        Flowable.<Integer>error(new ThrowingException()) //
                .doOnCancel(Actions.setToTrue(cancelled)) //
                .to(Transformers.reduce(reducer, 2)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
        assertTrue(cancelled.get());
    }

    @Test
    public void testErrorPostChaining() {
        Flowable.range(1, 100) //
                .concatWith(Flowable.<Integer>error(new ThrowingException())) //
                .to(Transformers.reduce(reducer, 2)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testMaxIterationsOne() {
        Function<Observable<Integer>, Observable<?>> tester = new Function<Observable<Integer>, Observable<?>>() {
            @Override
            public Observable<?> apply(Observable<Integer> o) throws Exception {
                return o.concatWith(Observable.<Integer>never());
            }
        };
        Flowable.just(1, 5) //
                .to(Transformers.repeat(plusOne, 3, 1, tester)) //
                .doOnNext(Consumers.println()) //
                .test() //
                .assertValues(2, 6) //
                .assertComplete();
    }

    @Test(timeout = 20000)
    public void testMaxIterationsTwoMaxChainedThree() {
        Flowable.just(1, 5) //
                .to(Transformers.reduce(plusOne, 3, 2)) //
                .test() //
                .assertValues(3, 7) //
                .assertComplete();
    }

    @Test(timeout = 20000)
    public void testMaxIterations() {
        Flowable.range(1, 2) //
                .to(Transformers.reduce(plusOne, 2, 3)) //
                .test() //
                .assertValues(4, 5) //
                .assertComplete();
    }

    @Test
    public void testDematerialize() {
        Flowable.just(Notification.createOnNext(1)).dematerialize().count().blockingGet();
        Flowable.empty().dematerialize().count().blockingGet();
    }

    @Test
    public void testBackpressure() {
        Flowable.range(1, 4) //
                .to(Transformers.reduce(plusOne, 2, 3)) //
                .test(0) //
                .assertNoValues() //
                .requestMore(1) //
                .assertValue(4) //
                .requestMore(1) //
                .assertValues(4, 5) //
                .requestMore(2) //
                .assertValues(4, 5, 6, 7) //
                .assertComplete();
    }

    private static void check(int n, int maxChained) {
        int result = Flowable.range(1, n) //
                .to(Transformers.reduce(reducer, maxChained)) //
                .doOnNext(Consumers.println()) //
                .single(-1) //
                .blockingGet();
        Assert.assertEquals(sum(n), result);
    }

    private static void checkAsync(int n, int maxChained) {
        int result = Flowable.range(1, n) //
                .to(Transformers.reduce(reducerAsync, maxChained)) //
                .single(-1) //
                .blockingGet();
        Assert.assertEquals(sum(n), result);
    }

    private static int sum(int n) {
        int sum = 0;
        for (int i = 1; i <= n; i++) {
            sum += i;
        }
        return sum;
    }

}
