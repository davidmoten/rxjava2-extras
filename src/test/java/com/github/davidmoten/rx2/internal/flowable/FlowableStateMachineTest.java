package com.github.davidmoten.rx2.internal.flowable;

import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.rx2.StateMachine.Emitter;
import com.github.davidmoten.rx2.StateMachine.Transition2;
import com.github.davidmoten.rx2.StateMachine2;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class FlowableStateMachineTest {

    @Test
    @Ignore
    public void test() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(new Transition2<String, Integer, Integer>() {

                    @Override
                    public String apply(String state, Integer value, Emitter<Integer> emitter) {
                        emitter.onNext_(value);
                        return state;
                    }
                }).build();

        Flowable.just(1, 2, 3) //
                .compose(sm) //
                .test() //
                .assertValues(1, 2, 3) //
                .assertComplete();
    }
}
