package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableReduce<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private final int depth;
    private final Action finished;

    public FlowableReduce(Flowable<T> source,
            Function<? super Flowable<T>, ? extends Flowable<T>> reducer, Action finished,
            int depth) {
        this.finished = finished;
        Preconditions.checkArgument(depth > 0, "depth must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.depth = depth;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        AtomicInteger numLevels = new AtomicInteger();

    }

    private static final class FlowableReduceSubscriber<T>
            implements FlowableSubscriber<T>, Subscription {

        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final int depth;
        private final Subscriber<? super T> child;

        private Subscription parent;

        FlowableReduceSubscriber(Flowable<T> source,
                Function<? super Flowable<T>, ? extends Flowable<T>> reducer, int depth,
                Subscriber<? super T> child) {
            this.reducer = reducer;
            this.depth = depth;
            this.child = child;
        }

        public void subscribe() {
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
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onComplete() {
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
    private static class ReduceReplaySubject<T> extends Flowable<T>
            implements FlowableSubscriber<T>, Subscription {

        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicLong unreconciledRequests = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscriber<? super T>> child = new AtomicReference<Subscriber<? super T>>();

        private Subscription parent;
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                unreconciledRequests.incrementAndGet();
                parent.request(1);
            }
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            // only one subscriber expected
            this.child.set(child);
            child.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                if (n < Long.MAX_VALUE) {
                    while (true) {
                        long p = unreconciledRequests.get();
                        long r = Math.max(0, n - p);
                        long p2 = p - (n - r);
                        if (unreconciledRequests.compareAndSet(p, p2)) {
                            parent.request(r);
                            break;
                        }
                    }
                } else {
                    parent.request(Long.MAX_VALUE);
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
                unreconciledRequests.incrementAndGet();
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
            if (wip.getAndIncrement() == 0) {
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
                    missed = wip.addAndGet(-missed);
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
