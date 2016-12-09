package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public final class StateMachine {

    private StateMachine() {
        // prevent instantiation
    }

    public interface Transition<State, In, Out>
            extends Function3<State, In, FlowableEmitter<Out>, State> {

        // override so IDEs have better suggestions for parameters
        @Override
        State apply(State state, In value, FlowableEmitter<Out> FlowableEmitter);

    }

    public interface Completion<State, Out> extends BiPredicate<State, FlowableEmitter<Out>> {

        // override so IDEs have better suggestions for parameters
        @Override
        boolean test(State state, FlowableEmitter<Out> FlowableEmitter);

    }

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

        public <In, Out> Builder3<State, In, Out> transition(
                Transition<State, In, Out> transition) {
            return new Builder3<State, In, Out>(initialState, transition);
        }

    }

    public static final class Builder3<State, In, Out> {

        private static final int DEFAULT_REQUEST_SIZE = 1;

        private final Callable<State> initialState;
        private final Transition<State, In, Out> transition;
        private Completion<State, Out> completion = CompletionAlwaysTrueHolder.instance();
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;
        private int requestBatchSize = DEFAULT_REQUEST_SIZE;

        private Builder3(Callable<State> initialState, Transition<State, In, Out> transition) {
            this.initialState = initialState;
            this.transition = transition;
        }

        public Builder3<State, In, Out> completion(Completion<State, Out> completion) {
            this.completion = completion;
            return this;
        }

        public Builder3<State, In, Out> backpressureStrategy(
                BackpressureStrategy backpressureStrategy) {
            this.backpressureStrategy = backpressureStrategy;
            return this;
        }

        public Builder3<State, In, Out> requestBatchSize(int value) {
            this.requestBatchSize = value;
            return this;
        }

        public FlowableTransformer<In, Out> build() {
            return Transformers.stateMachine(initialState, transition, completion,
                    backpressureStrategy, requestBatchSize);
        }

    }

    // visible for testing
    static final class CompletionAlwaysTrueHolder {

        private CompletionAlwaysTrueHolder() {
            // prevent instantiation
        }

        private static final Completion<Object, Object> INSTANCE = new Completion<Object, Object>() {
            @Override
            public boolean test(Object t1, FlowableEmitter<Object> t2) {
                return true;
            }
        };

        @SuppressWarnings("unchecked")
        static <State, Out> Completion<State, Out> instance() {
            return (Completion<State, Out>) INSTANCE;
        }
    }
}
