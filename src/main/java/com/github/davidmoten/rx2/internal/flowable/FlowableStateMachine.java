package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
import io.reactivex.internal.util.BackpressureHelper;
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

    private static final class StateMachineSubscriber<State, In, Out> extends AtomicInteger
            implements Subscriber<In>, Subscription, Emitter<Out> {
        private final Callable<? extends State> initialState;
        private final Function3<? super State, ? super In, ? super Emitter<Out>, ? extends State> transition;
        private final BiPredicate<? super State, ? super Emitter<Out>> completion;
        private final BackpressureStrategy backpressureStrategy;
        private final int requestBatchSize;
        private final SimpleQueue<Out> queue = new SpscLinkedArrayQueue<Out>(16);
        private final Subscriber<? super Out> child;
        private final AtomicLong requested = new AtomicLong();

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
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
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
            done_ = true;
            drain();
        }

        public void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            return;
                        }
                        Out t;
                        try {
                            t = queue.poll();
                        } catch (Throwable err) {
                            Exceptions.throwIfFatal(err);
                            cancelled = true;
                            queue.clear();
                            cancelled = true;
                            child.onError(err);
                            return;
                        }
                        if (t == null) {
                            if (done_) {
                                if (error_ != null) {
                                    // tODO
                                } else {
                                    // TODO
                                }
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (e > 0 && r != Long.MAX_VALUE) {
                        requested.addAndGet(-e);
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

    }
}
