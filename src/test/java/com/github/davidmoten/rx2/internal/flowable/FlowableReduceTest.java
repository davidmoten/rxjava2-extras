package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.junit.Assert;
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
        int result = Flowable.<Integer>empty() //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(-1, result);
    }

    @Test
    public void testOne() {
        int result = Flowable.just(10) //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(10, result);
    }

    @Test
    public void testCompletesFirstLevel() {
        int result = Flowable.just(1, 2) //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(3, result);
    }

    @Test
    public void testCompletesSecondLevel() {
        int result = Flowable.just(1, 2, 3) //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(6, result);
    }

    @Test
    public void testCompletesThirdLevel() {
        int result = Flowable.range(1, 4) //
                .to(Transformers.reduce(reducer, 2)) //
                .blockingGet(-1);
        Assert.assertEquals(10, result);
    }

}
