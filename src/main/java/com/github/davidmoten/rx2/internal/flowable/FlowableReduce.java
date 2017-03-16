package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.subjects.ReplaySubject;

public class FlowableReduce<T> extends Flowable<T> {

    private final Flowable<T> source;
    private Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private int depth;

    public FlowableReduce(Flowable<T> source,
            Function<? super Flowable<T>, ? extends Flowable<T>> reducer, int depth) {
        Preconditions.checkArgument(depth > 0, "depth must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.depth = depth;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        Flowable<T> src = source;
        new FlowableReduceSubscriber<T>(source, reducer, depth, child).subscribe();
    }

    private static final class FlowableReduceSubscriber<T>
            implements FlowableSubscriber<T>, Subscription {

        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final int depth;
        private final Subscriber<? super T> child;
        private final ReplaySubject<Object> subject;

        private Subscription parent;

        @SuppressWarnings("unchecked")
        FlowableReduceSubscriber(Flowable<T> source,
                Function<? super Flowable<T>, ? extends Flowable<T>> reducer, int depth,
                Subscriber<? super T> child) {
            this.reducer = reducer;
            this.depth = depth;
            this.child = child;
            this.subject = ReplaySubject.create();
        }

        void subscribe() {
            // TODO Auto-generated method stub

        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                parent.request(n);
            }
        }

        @Override
        public void onNext(T t) {
            subject.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            subject.onError(t);
        }

        @Override
        public void onComplete() {
            subject.onComplete();
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

    }

    /**
     * Requests minimally of upstream and buffers until this subscriber itself
     * is subscribed to.
     * 
     * @param <T>
     *            generic type
     */
    @SuppressWarnings("serial")
    private static class ReduceSwitchingSubscriber<T> extends AtomicInteger
            implements FlowableSubscriber<T>, Subscription {

        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicLong preSubscriptionRequests = new AtomicLong();
        private final AtomicReference<Subscriber<? super T>> child = new AtomicReference<Subscriber<? super T>>();

        private Subscription parent;
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                preSubscriptionRequests.incrementAndGet();
                parent.request(1);
            }
        }

        void subscribe(Subscriber<? super T> child) {
            this.child.set(child);
            child.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                while (true) {
                    long p = preSubscriptionRequests.get();
                    break;
                }
                drain();
            }
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);
            if (child.get() != null) {
                drain();
            } else {
                // make minimal request to keep upstream producing
                preSubscriptionRequests.incrementAndGet();
                parent.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            if (child.get() != null) {
                drain();
            }
        }

        @Override
        public void onComplete() {
            done = true;
            if (child.get() != null) {
                drain();
            }
        }

        private void drain() {
            if (child.get() != null && getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        boolean d = done;
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                parent.cancel();
                                Throwable err = error;
                                if (err != null) {
                                    error = null;
                                    child.get().onError(err);
                                } else {
                                    child.get().onComplete();
                                }
                            } else {
                                break;
                            }
                        } else {
                            child.get().onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        r = requested.addAndGet(-e);
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            parent.cancel();
        }

    }

}
