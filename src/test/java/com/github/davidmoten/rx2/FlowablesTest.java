package com.github.davidmoten.rx2;

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
        final BiFunction<Long, Long, Flowable<Long>> fetch = new BiFunction<Long, Long, Flowable<Long>>() {
            @Override
            public Flowable<Long> apply(Long start, Long request) {
                return Flowable.rangeLong(start, request);
            }

        };
        Flowables.fetchPagesByRequest(fetch) //
                .test(0) //
                .assertNoValues() //
                .requestMore(1) //
                .assertValue(0L) //
                .requestMore(2) //
                .assertValues(0L, 1L, 2L) //
                .requestMore(3) //
                .assertValues(0L, 1L, 2L, 3L, 4L, 5L) //
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
