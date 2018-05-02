package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;

public class FlowableMergeInterleavedTest {

    @Test
    public void testInterleaveTwoInfiniteStreams() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2, true) //
                .doOnNext(Consumers.println()) //
                .test(4)
                .assertValues(1, 1, 2, 2)
                .assertNotTerminated()
                .requestMore(3) //
                .assertValues(1, 1, 2, 2, 1, 1, 2);
    }

}
