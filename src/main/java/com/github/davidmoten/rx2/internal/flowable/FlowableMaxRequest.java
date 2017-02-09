package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMaxRequest<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final long maxRequest;

    public FlowableMaxRequest(Flowable<T> source, long maxRequest) {
        this.source = source;
        this.maxRequest = maxRequest;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new MaxRequestSubscriber<T>(maxRequest, child));
    }

    @SuppressWarnings("serial")
    private static final class MaxRequestSubscriber<T> extends AtomicInteger
            implements Subscriber<T>, Subscription {

        private final long maxRequest;
        private final Subscriber<? super T> child;
        private final AtomicLong requested = new AtomicLong();

        private Subscription parent;
        private long count;
        private volatile long nextRequest;
        private volatile boolean allArrived = true;

        MaxRequestSubscriber(long maxRequest, Subscriber<? super T> child) {
            this.maxRequest = maxRequest;
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
                requestMore();
            }
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void onNext(T t) {
            if (count != Long.MAX_VALUE) {
                count--;
                if (count == -1) {
                    // request didn't happen from this onNext method so refresh
                    // count from the volatile set in requestMore
                    long nr = nextRequest;
                    if (nr == Long.MAX_VALUE) {
                        count = nr;
                    } else {
                        count = nr - 1;
                    }
                }
                if (count == 0) {
                    while (true) {
                        long r = requested.get();
                        if (r == 0) {
                            // now must rely on dowstream requests to request
                            // more from upstream via `requestMore`
                            allArrived = true;
                            requestMore();
                            break;
                        }
                        long req = Math.min(r, maxRequest);
                        if (r == Long.MAX_VALUE) {
                            count = req;
                            parent.request(req);
                            break;
                        } else if (requested.compareAndSet(r, r - req)) {
                            count = req;
                            parent.request(req);
                            break;
                        }
                    }
                }
            }
            child.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            child.onError(t);

        }

        @Override
        public void onComplete() {
            child.onComplete();
        }

        private void requestMore() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    if (allArrived) {
                        while (true) {
                            long r = requested.get();
                            long req = Math.min(r, maxRequest);
                            if (r == 0) {
                                break;
                            } else if (r == Long.MAX_VALUE || requested.compareAndSet(r, r - req)) {
                                allArrived = false;
                                nextRequest = req;
                                parent.request(req);
                                break;
                            }
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
