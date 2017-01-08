package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.FlowableTransformers;

import io.reactivex.Flowable;

public final class FlowableReverseTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(FlowableReverse.class);
    }

    @Test
    public void testEmpty() {
        Flowable.empty() //
                .compose(FlowableTransformers.reverse()) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }

    @Test
    public void testOne() {
        Flowable.just(1) //
                .compose(FlowableTransformers.<Integer> reverse()) //
                .test() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testMany() {
        Flowable.just(1, 2, 3, 4, 5) //
                .compose(FlowableTransformers.<Integer> reverse()) //
                .test() //
                .assertValues(5, 4, 3, 2, 1) //
                .assertComplete();
    }

}
