package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;

import com.github.davidmoten.rx2.internal.flowable.TransformerStateMachine;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public final class Transformers {

    private Transformers() {
        // prevent instantiation
    }

    public static <State, In, Out> FlowableTransformer<In, Out> stateMachine(
            Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            BackpressureStrategy backpressureStrategy, int requestBatchSize) {
        return TransformerStateMachine.create(initialState, transition, completion,
                backpressureStrategy, requestBatchSize);
    }

    public static StateMachine.Builder stateMachine() {
        return StateMachine.builder();
    }

}
