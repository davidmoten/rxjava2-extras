package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;

public class FlowableMergeInterleavedTest {

    @Test(timeout = 3000)
    public void testInterleave() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2) //
                .doOnNext(Consumers.println()) //
                .test(4)
                .assertValues(1, 1, 2, 2)
                .assertNotTerminated();
    }

}
