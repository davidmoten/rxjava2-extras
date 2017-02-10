package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMinRequest<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final int minRequest;
    private final boolean constrainFirstRequest;

    public FlowableMinRequest(Flowable<T> source, int minRequest, boolean constrainFirstRequest) {
        Preconditions.checkArgument(minRequest > 0, "minRequest must be >0");
        this.source = source;
        this.minRequest = minRequest;
        this.constrainFirstRequest = constrainFirstRequest;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new MinRequestSubscriber<T>(minRequest, constrainFirstRequest, child));
    }

    @SuppressWarnings("serial")
    private static final class MinRequestSubscriber<T> extends AtomicInteger
            implements Subscriber<T>, Subscription {

        private final int minRequest;
        private final boolean constrainFirstRequest;
        private final Subscriber<? super T> child;
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Subscription parent;
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private long count;
        private boolean firstRequest = true;

        MinRequestSubscriber(int minRequest, boolean constrainFirstRequest,
                Subscriber<? super T> child) {
            this.minRequest = minRequest;
            this.constrainFirstRequest = constrainFirstRequest;
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
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
        public void onNext(T t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable e) {
            error = e;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        private void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    boolean d = done;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                terminate();
                                return;
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                            if (count != Long.MAX_VALUE) {
                                count--;
                            }
                        }
                        d = done;
                    }
                    if (d && queue.isEmpty()) {
                        terminate();
                        return;
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        r = requested.addAndGet(-e);
                    }
                    if (r != 0 && count == 0) {
                        if (firstRequest && !constrainFirstRequest) {
                            count = r;
                            parent.request(r);
                            firstRequest = false;
                        } else {
                            count = Math.max(r, minRequest);
                            parent.request(count);
                        }
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        private void terminate() {
            parent.cancel();
            Throwable err = error;
            if (err != null) {
                error = null;
                child.onError(err);
            } else {
                child.onComplete();
            }
        }
    }

}
