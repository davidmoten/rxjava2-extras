package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableMinRequest<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final long minRequest;

    public FlowableMinRequest(Flowable<T> source, long minRequest) {
        this.source = source;
        this.minRequest = minRequest;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new MinRequestSubscriber<T>(minRequest, child));
    }

    @SuppressWarnings("serial")
    private static final class MinRequestSubscriber<T> extends AtomicInteger implements Subscriber<T>, Subscription {

        private final long minRequest;
        private final Subscriber<? super T> child;
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Subscription parent;
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private long count;
        private boolean firstRequest = true;

        public MinRequestSubscriber(long minRequest, Subscriber<? super T> child) {
            this.minRequest = minRequest;
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
                    while (e != r) {
                        if (cancelled) {
                            return;
                        }
                        boolean d = done;
                        T t = queue.poll();
                        if (t == null) {
                            if (done) {
                                parent.cancel();
                                Throwable err = error;
                                if (err != null) {
                                    error = null;
                                    child.onError(err);
                                } else {
                                    child.onComplete();
                                }
                                return;
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
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
