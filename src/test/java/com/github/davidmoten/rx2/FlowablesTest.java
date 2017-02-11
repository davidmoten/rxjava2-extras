package com.github.davidmoten.rx2;

import java.util.function.LongFunction;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;

public class FlowablesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Flowables.class);
    }

    @Test
    public void testFetchByRequest() {
        final BiFunction<Long, Long, Flowable<Integer>> fetch = new BiFunction<Long, Long, Flowable<Integer>>() {
            @Override
            public Flowable<Integer> apply(Long start, Long n) {
                return Flowable.just(n.intValue()).repeat(n);
            }

        };
        Flowables.fetchPagesByRequest(fetch) //
                .test(0) //
                .assertNoValues() //
                .requestMore(1) //
                .assertValue(1) //
                .requestMore(2) //
                .assertValues(1, 2, 2) //
                .requestMore(3) //
                .assertValues(1, 2, 2, 3, 3, 3) //
                .assertNotTerminated();
    }

    @Test
    public void testFetchByRequestError() {
        final BiFunction<Long, Long, Flowable<Integer>> fetch = new BiFunction<Long, Long, Flowable<Integer>>() {
            @Override
            public Flowable<Integer> apply(Long start, Long n) {
                throw new ThrowingException();
            }

        };
        Flowables.fetchPagesByRequest(fetch) //
                .test(1) //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

}
