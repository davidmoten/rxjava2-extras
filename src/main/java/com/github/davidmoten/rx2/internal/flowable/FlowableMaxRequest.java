package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMaxRequest<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final long[] maxRequests;

    public FlowableMaxRequest(Flowable<T> source, long[] maxRequests) {
        Preconditions.checkArgument(maxRequests.length > 0, "maxRequests length must be greater than 0");
        for (int i = 0; i < maxRequests.length; i++) {
            Preconditions.checkArgument(maxRequests[i] > 0, "maxRequests items must be greater than zero");
        }
        this.source = source;
        this.maxRequests = maxRequests;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new MaxRequestSubscriber<T>(maxRequests, child));
    }

    @SuppressWarnings("serial")
    private static final class MaxRequestSubscriber<T> extends AtomicInteger implements FlowableSubscriber<T>, Subscription {

        private final long[] maxRequests;
        private int requestNum;
        private final Subscriber<? super T> child;

        // the number of requests from downstream that have not been requested
        // from upstream yet
        private final AtomicLong requested = new AtomicLong();

        // the upstream subscription (which allows us to request from upstream
        // and cancel it)
        private Subscription parent;

        // the number of items still to be emitted from
        // upstream out of the last request to parent
        private long count;

        // when request made from `requestMore` this value is used to set the
        // next value of `count` in the `onNext` method
        private volatile long nextRequest;

        // indicates to the `requestMore` method that all items from the last
        // request to parent have arrived
        private volatile boolean allArrived = true;

        MaxRequestSubscriber(long[] maxRequests, Subscriber<? super T> child) {
            this.maxRequests = maxRequests;
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
                    // count from the volatile set in `requestMore`
                    long nr = nextRequest;
                    if (nr == Long.MAX_VALUE) {
                        count = nr;
                    } else {
                        count = nr - 1;
                    }
                }
                if (count == 0) {
                    // All items from the last request made to parent have
                    // arrived

                    // CAS loop to update `requested`
                    long mr = peekNextMaxRequest();
                    while (true) {
                        long r = requested.get();
                        if (r == 0) {
                            // now must rely on dowstream requests to request
                            // more from upstream via `requestMore`
                            allArrived = true;
                            requestMore();
                            break;
                        } else if (r == Long.MAX_VALUE) {
                            nextMaxRequest();
                            count = mr;
                            parent.request(mr);
                            break;
                        } else {
                            long req = Math.min(r, mr);
                            if (requested.compareAndSet(r, r - req)) {
                                nextMaxRequest();
                                count = req;
                                parent.request(req);
                                break;
                            }
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
                        // CAS loop to update requested
                        long mr = peekNextMaxRequest();
                        while (true) {
                            long r = requested.get();
                            long req = Math.min(r, mr);
                            if (r == 0) {
                                break;
                            } else if (r == Long.MAX_VALUE || requested.compareAndSet(r, r - req)) {
                                nextMaxRequest();
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

        private long peekNextMaxRequest() {
            return maxRequests[requestNum];
        }

        private void nextMaxRequest() {
            if (requestNum != maxRequests.length - 1) {
                requestNum++;
            }
        }

    }

}
