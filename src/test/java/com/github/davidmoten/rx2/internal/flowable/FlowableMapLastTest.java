package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Transformers;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

public class FlowableMapLastTest {

    @Test
    public void testMapLastRequestAmount() {
        List<Long> list = new ArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(list))//
                .compose(Transformers.mapLast(new Function<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {
                        return x + 1;
                    }
                })).test(1) //
                .assertNotComplete() //
                .assertValues(1) //
                .requestMore(3) //
                .assertValues(1, 2, 3, 4);
        assertEquals(Arrays.asList(2L, 3L), list);
    }

    @Test
    public void testMapLastHandlesRequestOverflow() {
        List<Long> list = new ArrayList<Long>();
        Flowable.range(1, 5) //
                .doOnRequest(Consumers.addLongTo(list))//
                .compose(Transformers.mapLast(new Function<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {
                        return x + 1;
                    }
                })).test()//
                .assertComplete() //
                .assertValues(1, 2, 3, 4, 6);
        assertEquals(Arrays.asList(Long.MAX_VALUE), list);
    }

    @Test(expected = OutOfMemoryError.class)
    public void testMapLastHandlesFatalError() {
        Flowable.range(1, 5) //
                .compose(Transformers.mapLast(new Function<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {
                        throw new OutOfMemoryError();
                    }
                })).subscribe();
    }

    @Test
    public void testMapLastHandlesNonFatalError() {
        final RuntimeException e = new RuntimeException();
        Flowable.range(1, 5) //
                .compose(Transformers.mapLast(new Function<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer x) {
                        throw e;
                    }
                })).test() //
                .assertError(e);
    }

}
