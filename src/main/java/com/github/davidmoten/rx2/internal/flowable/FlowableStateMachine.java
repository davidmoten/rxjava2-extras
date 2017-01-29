package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.StateMachine.Emitter;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;
import io.reactivex.internal.fuseable.SimpleQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.plugins.RxJavaPlugins;

public class FlowableStateMachine<State, In, Out> extends Flowable<Out> {

    private final Flowable<In> source;
    private final Callable<? extends State> initialState;
    private final Function3<? super State, ? super In, ? super Emitter<Out>, ? extends State> transition;
    private final BiPredicate<? super State, ? super Emitter<Out>> completion;
    private final BackpressureStrategy backpressureStrategy;
    private final int requestBatchSize;

    public FlowableStateMachine(Flowable<In> source, Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super Emitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super Emitter<Out>> completion, BackpressureStrategy backpressureStrategy,
            int requestBatchSize) {
        Preconditions.checkNotNull(initialState);
        Preconditions.checkNotNull(transition);
        Preconditions.checkNotNull(completion);
        Preconditions.checkNotNull(backpressureStrategy);
        Preconditions.checkArgument(requestBatchSize > 0, "initialRequest must be greater than zero");
        this.source = source;
        this.initialState = initialState;
        this.transition = transition;
        this.completion = completion;
        this.backpressureStrategy = backpressureStrategy;
        this.requestBatchSize = requestBatchSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super Out> child) {
        source.subscribe(new StateMachineSubscriber<State, In, Out>(initialState, transition, completion,
                backpressureStrategy, requestBatchSize, child));
    }

    private static final class StateMachineSubscriber<State, In, Out>
            implements Subscriber<In>, Subscription, Emitter<Out> {
        private final Callable<? extends State> initialState;
        private final Function3<? super State, ? super In, ? super Emitter<Out>, ? extends State> transition;
        private final BiPredicate<? super State, ? super Emitter<Out>> completion;
        private final BackpressureStrategy backpressureStrategy;
        private final int requestBatchSize;
        private final SimpleQueue<Out> queue = new SpscLinkedArrayQueue<Out>(16);
        private final Subscriber<? super Out> child;

        private Subscription parent;
        private volatile boolean cancelled;
        private State state;
        private boolean done;
        private volatile boolean done_;
        private Throwable error_;

        StateMachineSubscriber(Callable<? extends State> initialState,
                Function3<? super State, ? super In, ? super Emitter<Out>, ? extends State> transition,
                BiPredicate<? super State, ? super Emitter<Out>> completion, BackpressureStrategy backpressureStrategy,
                int requestBatchSize, Subscriber<? super Out> child) {
            this.initialState = initialState;
            this.transition = transition;
            this.completion = completion;
            this.backpressureStrategy = backpressureStrategy;
            this.requestBatchSize = requestBatchSize;
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                child.onSubscribe(parent);
                this.parent = parent;
            }
        }

        @Override
        public void onNext(In t) {
            if (done) {
                return;
            }
            if (state == null) {
                try {
                    state = initialState.call();
                } catch (Exception e) {
                    Exceptions.throwIfFatal(e);
                    onError(e);
                    return;
                }
            }
            try {
                state = transition.apply(state, t, this);
            } catch (Exception e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
        }

        @Override
        public void onError(Throwable e) {
            if (done) {
                RxJavaPlugins.onError(e);
                return;
            }
            done = true;

        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            try {
                if (completion.test(state, this)) {
                    done_ = true;
                }
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
            done = true;
        }

        @Override
        public void request(long n) {
            // TODO Auto-generated method stub

        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void onNext_(Out t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError_(Throwable e) {
            if (done_) {
                RxJavaPlugins.onError(e);
                return;
            }
            error_ = e;
            done_ = true;
            drain();
        }

        @Override
        public void onComplete_() {
            if (done_) {
                return;
            }
            done = true;
            drain();
        }
        
        public void drain() {
            
        }

    }
}
