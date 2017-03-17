package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

public final class FlowableReduceTest {

    static final Function<List<Integer>, Integer> sum = (new Function<List<Integer>, Integer>() {
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

    static final Function<Flowable<Integer>, Flowable<Integer>> reducer = new Function<Flowable<Integer>, Flowable<Integer>>() {

        @Override
        public Flowable<Integer> apply(Flowable<Integer> f) throws Exception {
            return f.buffer(2).map(sum);
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

    @Test(timeout = 200000000)
    public void testCompletesFourLevels() {
        check(8, 2);
    }

    @Test
    public void testCompletesVariableLevelsWithVaryingDepths() {
        for (int maxDepthConcurrent = 1; maxDepthConcurrent < 5; maxDepthConcurrent++) {
            for (int n = 5; n <= 100; n++) {
                check(n, maxDepthConcurrent);
            }
        }
    }

    private static void check(int n, int maxDepthConcurrent) {
        int result = Flowable.range(1, n) //
                .to(Transformers.reduce(reducer, maxDepthConcurrent)) //
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
