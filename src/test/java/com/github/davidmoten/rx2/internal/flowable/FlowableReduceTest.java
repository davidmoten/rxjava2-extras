package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

public final class FlowableReduceTest {

    private static final Function<List<Integer>, Integer> sum = (new Function<List<Integer>, Integer>() {
        @Override
        public Integer apply(List<Integer> list) throws Exception {
            return list.stream().collect(Collectors.summingInt( //
                    new ToIntFunction<Integer>() {
                @Override
                public int applyAsInt(Integer x) {
                    return x;
                }
            }));
        }
    });

    private static final Function<Flowable<Integer>, Flowable<Integer>> reducer = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            return f.buffer(2).map(sum);
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
            if (count.incrementAndGet()>= 3) {
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
        int result = Flowable.<Integer> empty() //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(-1, result);
    }

    @Test
    public void testOne() {
        check(1, 2);
    }

    @Test
    public void testCompletesFirstLevel() {
        check(2, 2);
    }

    @Test
    public void testCompletesSecondLevel() {
        check(3, 2);
    }

    @Test
    public void testCompletesThirdLevel() {
        check(4, 2);
    }

    @Test
    public void testCompletesFourLevels() {
        check(8, 2);
    }

    @Test
    public void testMany() {
        for (int maxChained = 1; maxChained < 5; maxChained++) {
            for (int n = 5; n <= 100; n++) {
                check(n, maxChained);
            }
        }
    }

    @Test
    public void testManyAsync() {
        for (int maxChained = 1; maxChained < 5; maxChained++) {
            for (int n = 5; n <= 100; n++) {
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

    private static void check(int n, int maxChained) {
        int result = Flowable.range(1, n) //
                .to(Transformers.reduce(reducer, maxChained)) //
                .blockingGet(-1);
        Assert.assertEquals(sum(n), result);
    }

    private static void checkAsync(int n, int maxChained) {
        int result = Flowable.range(1, n) //
                .to(Transformers.reduce(reducerAsync, maxChained)) //
                .blockingGet(-1);
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
