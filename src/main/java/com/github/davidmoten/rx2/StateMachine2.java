package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;

import com.github.davidmoten.rx2.StateMachine.Completion2;
import com.github.davidmoten.rx2.StateMachine.Errored;
import com.github.davidmoten.rx2.StateMachine.Transition2;
import com.github.davidmoten.rx2.internal.flowable.FlowableStateMachine;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

public class StateMachine2 {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private Builder() {
            // prevent instantiation from other packages
        }

        public <State> Builder2<State> initialStateFactory(Callable<State> initialState) {
            return new Builder2<State>(initialState);
        }

        public <State> Builder2<State> initialState(final State initialState) {
            return initialStateFactory(Callables.constant(initialState));
        }

    }

    public static final class Builder2<State> {

        private final Callable<State> initialState;

        private Builder2(Callable<State> initialState) {
            this.initialState = initialState;
        }

        public <In, Out> Builder3<State, In, Out> transition(Transition2<State, In, Out> transition) {
            return new Builder3<State, In, Out>(initialState, transition);
        }

    }

    public static final class Builder3<State, In, Out> {

        private static final int DEFAULT_REQUEST_SIZE = 1;

        private final Callable<State> initialState;
        private final Transition2<State, In, Out> transition;
        private Completion2<State, Out> completion = null;
        private Errored<State, Out> errored = null;
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;
        private int requestBatchSize = DEFAULT_REQUEST_SIZE;

        private Builder3(Callable<State> initialState, Transition2<State, In, Out> transition) {
            this.initialState = initialState;
            this.transition = transition;
        }

        public Builder3<State, In, Out> completion(Completion2<State, Out> completion) {
            this.completion = completion;
            return this;
        }

        public Builder3<State, In, Out> errored(Errored<State, Out> errored) {
            this.errored = errored;
            return this;
        }

        public Builder3<State, In, Out> backpressureStrategy(BackpressureStrategy backpressureStrategy) {
            this.backpressureStrategy = backpressureStrategy;
            return this;
        }

        public Builder3<State, In, Out> requestBatchSize(int value) {
            this.requestBatchSize = value;
            return this;
        }

        public FlowableTransformer<In, Out> build() {
            return new FlowableTransformer<In, Out>() {

                @Override
                public Publisher<Out> apply(Flowable<In> source) {
                    return new FlowableStateMachine<State, In, Out>(source, initialState, transition, completion,
                            errored, backpressureStrategy, requestBatchSize);
                }
            };
        }

    }

}
