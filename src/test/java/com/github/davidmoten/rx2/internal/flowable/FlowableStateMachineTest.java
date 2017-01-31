package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.github.davidmoten.rx2.StateMachine.Completion2;
import com.github.davidmoten.rx2.StateMachine.Emitter;
import com.github.davidmoten.rx2.StateMachine.Transition2;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.StateMachine2;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public final class FlowableStateMachineTest {
    
    private static final Transition2<String, Integer, Integer> PASS_THROUGH_TRANSITION = new Transition2<String, Integer, Integer>() {

        @Override
        public String apply(String state, Integer value, Emitter<Integer> emitter) {
            emitter.onNext_(value);
            return state;
        }
    };

    private static FlowableTransformer<Integer, Integer> passThrough(int batchSize) {

        return StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .requestBatchSize(batchSize) //
                .build();
    }

    @Test
    public void testRequestBatchSize1() {
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(passThrough(1)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testPassThroughWithCustomCompletion() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                        emitter.onComplete_();
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(sm) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }
    
    @Test
    public void testCompletionThrows() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                       throw new ThrowingException();
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1) //
                .compose(sm) //
                .test() //
                .assertValues(1) //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testRequestBatchSize2() {
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(passThrough(2)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }
    
    @Test
    public void testErrorPassThrough() {
        RuntimeException e = new RuntimeException();
        Flowable.<Integer>error(e) //
                .compose(passThrough(1)) //
                .test() //
                .assertNoValues() //
                .assertError(e);
    }

    @Test
    public void testRequestBatchSize1WithBackpressure() {
        Flowable.just(1, 2) //
                .compose(passThrough(1)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValues(1, 2) //
                .requestMore(1) //
                .assertComplete();
    }

    @Test
    public void testRequestBatchSize1WithBackpressureRequestMoreThanAvailable() {
        Flowable.just(1, 2) //
                .compose(passThrough(1)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(100) //
                .assertValues(1, 2) //
                .assertComplete();
    }

    @Test
    public void testRequestBatchSize2WithBackpressure() {
        Flowable.just(1, 2) //
                .compose(passThrough(2)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValues(1, 2) //
                .requestMore(1) //
                .assertComplete();
    }

    @Test
    public void testStateFactoryReturnsNull() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialStateFactory(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return null;
                    }
                }) //
                .transition(PASS_THROUGH_TRANSITION) //
                .build();
        Flowable.just(1) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testStateFactoryReturnsNullOnEmptySource() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialStateFactory(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return null;
                    }
                }) //
                .transition(PASS_THROUGH_TRANSITION) //
                .build();
        Flowable.<Integer> empty() //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

}
