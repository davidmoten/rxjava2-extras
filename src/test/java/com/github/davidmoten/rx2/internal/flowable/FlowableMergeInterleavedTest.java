package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public final class FlowableMergeInterleavedTest {

    @Test
    public void testInterleaveTwoInfiniteStreams() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2, true) //
                .test(4) //
                .assertValues(1, 1, 2, 2) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveTwoInfiniteStreamsRequestOne() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2, true) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveTwoInfiniteStreamsRequestFive() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test(5) //
                .assertValues(1, 2, 1, 2, 1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveOneStream() {
        Flowable<Integer> a = Flowable.just(1).repeat(6);
        Flowables.mergeInterleaved(Flowable.just(a), 2, 2, true) //
                .test(3) //
                .assertValues(1, 1, 1) //
                .assertNotTerminated() //
                .requestMore(2) //
                .assertValues(1, 1, 1, 1, 1) //
                .requestMore(100) //
                .assertValueCount(6) //
                .assertComplete();
    }

    @Test
    public void testInterleaveOneStreamEmpty() {
        Flowable<Integer> a = Flowable.empty();
        Flowables.mergeInterleaved(Flowable.just(a), 2, 2, true) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }

    @Test
    public void testInterleaveInfiniteStreamWithFiniteStream() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2, 2);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test(6) //
                .assertValues(1, 2, 1, 2, 1, 1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveInfiniteStreamWithNever() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.never();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test(3) //
                .assertValues(1, 1, 1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveInfiniteStreamWithNeverReversed() {
        Flowable<Integer> a = Flowable.never();
        Flowable<Integer> b = Flowable.just(1).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test(3) //
                .assertValues(1, 1, 1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveTwoCompletingStreamsSameSize() {
        Flowable<Integer> a = Flowable.just(1, 1);
        Flowable<Integer> b = Flowable.just(2, 2);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test() //
                .assertValues(1, 2, 1, 2) //
                .assertComplete();
    }

    @Test
    public void testInterleaveCompletingStreamsDifferentSize() {
        Flowable<Integer> a = Flowable.just(1, 1, 1);
        Flowable<Integer> b = Flowable.just(2, 2);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test() //
                .assertValues(1, 2, 1, 2, 1) //
                .assertComplete();
    }

    @Test
    public void testInterleaveCompletingStreamsWithEmpty() {
        Flowable<Integer> a = Flowable.just(1, 1, 1);
        Flowable<Integer> b = Flowable.empty();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test() //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

    @Test
    public void testMergeWithErrorDelayed() {
        Flowable<Integer> a = Flowable.just(1, 1, 1);
        RuntimeException e = new RuntimeException();
        Flowable<Integer> b = Flowable.error(e);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .test() //
                .assertValues(1, 1, 1) //
                .assertError(e);
    }

    @Test
    public void testMergeWithErrorNoDelay() {
        Flowable<Integer> a = Flowable.just(1, 1, 1);
        RuntimeException e = new RuntimeException();
        Flowable<Integer> b = Flowable.error(e);
        Flowables.mergeInterleaved(Flowable.just(a, b)) //
                .maxConcurrency(2) //
                .batchSize(1) //
                .delayErrors(false) //
                .build() //
                .doOnNext(Consumers.println()) //
                .test() //
                .assertNoValues() //
                .assertError(e);
    }

    @Test
    @Ignore
    public void testInterleaveAsync() {
        Flowable<Integer> a = Flowable.just(1).repeat(100).subscribeOn(Schedulers.io());
        Flowable<Integer> b = Flowable.just(2).repeat(100);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2, true) //
                .test() //
                .assertValueCount(200) //
                .assertComplete();
    }

}
