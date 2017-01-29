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
import io.reactivex.internal.functions.ObjectHelper;
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

    @SuppressWarnings("serial")
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
        private long count; // counts down arrival of last request batch
        private volatile boolean requestsArrived; // communicates to drainLoop
                                                  // that we can request more if
                                                  // needed

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
                this.count = requestBatchSize;
                parent.request(requestBatchSize);
            }
        }

        @Override
        public void onNext(In t) {
            if (done) {
                return;
            }
            if (!createdState()) {
                return;
            }
            count -= 1;
            if (count == 0) {
                requestsArrived = true;
                count = requestBatchSize;
            }
            try {
                state = ObjectHelper.requireNonNull(transition.apply(state, t, this),
                        "intermediate state cannot be null");
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
            // TODO don't need to drain if drain occurred in transition
            drain();
        }

        private boolean createdState() {
            if (state == null) {
                try {
                    state = ObjectHelper.requireNonNull(initialState.call(), "initial state cannot be null");
                    return true;
                } catch (Throwable e) {
                    Exceptions.throwIfFatal(e);
                    onError(e);
                    return false;
                }
            } else {
                return true;
            }
        }

        @Override
        public void onError(Throwable e) {
            onError_(e);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            if (!createdState()) {
                return;
            }
            try {
                if (completion.test(state, this)) {
                    done = true;
                    onComplete_();
                }
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
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
        public void cancel_() {
            cancel();
        }

        @Override
        public void onNext_(Out t) {
            if (done_) {
                return;
            }
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
                    boolean reqsArrived;
                    if (r != Long.MAX_VALUE) {
                        reqsArrived = requestsArrived;
                    } else {
                        reqsArrived = false;
                    }
                    while (e != r) {
                        if (cancelled) {
                            return;
                        }
                        Out t;
                        try {
                            t = queue.poll();
                        } catch (Throwable err) {
                            Exceptions.throwIfFatal(err);
                            cancel();
                            queue.clear();
                            child.onError(err);
                            return;
                        }
                        if (t == null) {
                            if (done_) {
                                Throwable err = error_;
                                if (err != null) {
                                    cancel();
                                    queue.clear();
                                    child.onError(err);
                                    return;
                                } else {
                                    cancel();
                                    queue.clear();
                                    child.onComplete();
                                    return;
                                }
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (r != Long.MAX_VALUE) {
                        if (e != 0) {
                            requested.addAndGet(-e);
                        }
                        if (e != r && reqsArrived) {
                            requestsArrived = false;
                            parent.request(requestBatchSize);
                        }
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
