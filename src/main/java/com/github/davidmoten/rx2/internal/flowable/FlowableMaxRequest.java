package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableMaxRequest<T> extends Flowable<T> {

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

    private static final class MaxRequestSubscriber<T> extends AtomicInteger implements Subscriber<T>, Subscription {

        private final long maxRequest;
        private final Subscriber<? super T> child;
        private final AtomicLong requested = new AtomicLong();
        private Subscription parent;
        private long count;
        private volatile long currentRequest;

        public MaxRequestSubscriber(long maxRequest, Subscriber<? super T> child) {
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
            }
        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

        @Override
        public void onNext(T t) {
            if (--count == 0) {
                long r = requested.get();
                if (r > 0) {
                    long req = Math.min(r, maxRequest);
                    requested.addAndGet(-req);
                    parent.request(req);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onComplete() {
            // TODO Auto-generated method stub

        }
     
        private void requestMore() {
            if (getAndIncrement()==0) {
                int missed = 1;
                while (true) {
                    break;
                }
                missed = addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
        
    }

}
