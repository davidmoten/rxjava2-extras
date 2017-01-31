package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.github.davidmoten.rx2.StateMachine.Emitter;
import com.github.davidmoten.rx2.StateMachine.Transition2;
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
    public void testRequestBatchSize2() {
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(passThrough(2)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
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

}
