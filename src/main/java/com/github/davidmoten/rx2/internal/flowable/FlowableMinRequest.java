package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
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

        private Subscription parent;

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
            parent.cancel();
        }

        @Override
        public void onNext(T t) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onError(Throwable t) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onComplete() {
            // TODO Auto-generated method stub

        }
        
        private void drain() {
            
        }

    }

}
