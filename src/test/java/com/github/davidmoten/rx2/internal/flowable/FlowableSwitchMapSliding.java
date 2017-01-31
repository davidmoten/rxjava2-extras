package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

public class FlowableSwitchMapSliding<T> extends Flowable<T> {

    private final Flowable<Flowable<T>> sources;
    private final int size;
    private final int requestBatchSize;

    public FlowableSwitchMapSliding(Flowable<Flowable<T>> sources, int size, int requestBatchSize) {
        this.sources = sources;
        this.size = size;
        this.requestBatchSize = requestBatchSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {

    }

    private static final class SourcesSubscriber<T> implements Subscriber<Flowable<T>>, Subscription {

        private final Flowable<Flowable<T>> sources;
        private final int size;
        private final int requestBatchSize;
        private final Subscriber<? super T> child;

        private Subscription parent;
        private int nextIndex;
        private SourceSubscriber<T>[] subscribers;
        private int count;

        @SuppressWarnings("unchecked")
        SourcesSubscriber(Flowable<Flowable<T>> sources, int size, int requestBatchSize, Subscriber<? super T> child) {
            this.sources = sources;
            this.size = size;
            this.requestBatchSize = requestBatchSize;
            this.child = child;
            this.subscribers = new SourceSubscriber[requestBatchSize + 1];
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
                parent.request(size);
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                // TODO
            }
        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

        @Override
        public void onNext(Flowable<T> source) {
            SourceSubscriber<T> s = new SourceSubscriber<T>();
            subscribers[nextIndex] = s;
            nextIndex = (nextIndex + 1) % (size);
            source.subscribe(s);
            s.request(requestBatchSize);
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

    private static final class SourceSubscriber<T> implements Subscriber<T>, Subscription {

        public SourceSubscriber() {
            // TODO Auto-generated constructor stub
        }

        @Override
        public void onSubscribe(Subscription s) {
            // TODO Auto-generated method stub

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

        @Override
        public void request(long n) {
            // TODO Auto-generated method stub

        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

    }

}
