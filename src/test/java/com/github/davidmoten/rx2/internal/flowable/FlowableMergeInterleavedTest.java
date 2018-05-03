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
                .test(4) //
                .assertValues(1, 1, 2, 2) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveTwoInfiniteStreamsRequestOne() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 2, true) //
                .doOnNext(Consumers.println()) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated();
    }

    @Test
    public void testInterleaveTwoInfiniteStreamsRequestFive() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .doOnNext(Consumers.println()) //
                .test(5) //
                .assertValues(1, 2, 1, 2, 1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInterleaveOneStream() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowables.mergeInterleaved(Flowable.just(a), 2, 2, true) //
                .doOnNext(Consumers.println()) //
                .test(5) //
                .assertValues(1, 1, 1, 1, 1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInterleaveInfiniteStreamWithFiniteStream() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.just(2, 2);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .doOnNext(Consumers.println()) //
                .test(6) //
                .assertValues(1, 2, 1, 2, 1, 1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInterleaveInfiniteStreamWithNever() {
        Flowable<Integer> a = Flowable.just(1).repeat();
        Flowable<Integer> b = Flowable.never();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .doOnNext(Consumers.println()) //
                .test(3) //
                .assertValues(1, 1, 1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInterleaveInfiniteStreamWithNeverReversed() {
        Flowable<Integer> a = Flowable.never();
        Flowable<Integer> b = Flowable.just(1).repeat();
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .doOnNext(Consumers.println()) //
                .test(3) //
                .assertValues(1, 1, 1) //
                .assertNotTerminated();
    }
    
    @Test
    public void testInterleaveTwoCompletingStreamsSameSize() {
        Flowable<Integer> a = Flowable.just(1, 1, 1);
        Flowable<Integer> b = Flowable.just(2, 2, 2);
        Flowables.mergeInterleaved(Flowable.just(a, b), 2, 1, true) //
                .doOnNext(Consumers.println()) //
                .test() //
                .assertValues(1, 2, 1, 2, 1, 2) //
                .assertComplete();
    }


}
