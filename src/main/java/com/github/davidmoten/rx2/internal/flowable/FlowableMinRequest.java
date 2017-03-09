package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMinRequest<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final int[] minRequest;

    public FlowableMinRequest(Flowable<T> source, int[] minRequests) {
        Preconditions.checkArgument(minRequests.length > 0, "minRequests length must be > 0");
        for (int i = 0; i < minRequests.length; i++) {
            Preconditions.checkArgument(minRequests[i] > 0, "each item in minRequests must be > 0");
        }
        this.source = source;
        this.minRequest = minRequests;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new MinRequestSubscriber<T>(minRequest, child));
    }

    @SuppressWarnings("serial")
    private static final class MinRequestSubscriber<T> extends AtomicInteger implements FlowableSubscriber<T>, Subscription {

        private final int[] minRequests;
        private int requestNum;
        private final Subscriber<? super T> child;
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Subscription parent;
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private long count;

        MinRequestSubscriber(int[] minRequests, Subscriber<? super T> child) {
            this.minRequests = minRequests;
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
                        // requests from parent have arrived so request some
                        // more
                        int min = minRequests[requestNum];
                        if (requestNum != minRequests.length - 1) {
                            requestNum++;
                        }
                        count = Math.max(r, min);
                        parent.request(count);
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
