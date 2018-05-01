package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;

public class FlowableMergeInterleavedTest {

    @Test
    public void testInterleave() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 4) //
                .doOnNext(Consumers.println()) //
                .test(24);
    }

}
