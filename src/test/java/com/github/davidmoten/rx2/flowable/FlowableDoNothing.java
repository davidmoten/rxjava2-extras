package com.github.davidmoten.rx2.flowable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

public final class FlowableDoNothing<T> extends Flowable<T> {

    private final Flowable<T> source;

    public FlowableDoNothing(Flowable<T> source) {
        this.source = source;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> downstream) {
        source.subscribe(new DoNothingSubscriber<T>(downstream));
    }

    static final class DoNothingSubscriber<T> implements //
            // receives onSubscribe then onNext + terminal events from upstream
            FlowableSubscriber<T>, //
            // receives request and cancellation calls from downstream
            Subscription {

        private final Subscriber<? super T> downstream;
        private Subscription upstream;

        DoNothingSubscriber(Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Subscription upstream) {
            // this method is responsible for notifying downstream
            // where to send requests and cancellation by calling
            // downstream.onSubscribe with a Subscription object
            // (we reuse `this`)

            if (SubscriptionHelper.validate(this.upstream, upstream)) {
                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            // pass emission through to downstream

            // note downstream is not null because onSubscribe call
            // must finish before events arrive (contractual)
            downstream.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            // pass emission through to downstream
            downstream.onError(t);
        }

        @Override
        public void onComplete() {
            // pass emission through to downstream
            downstream.onComplete();
        }

        @Override
        public void request(long n) {
            // requests received from downstream are passed to upstream

            // note that upstream is non-null because by specification
            // onSubscribe must terminate before request or cancel is
            // called on the Subscription passed into onSubscribe.
            if (SubscriptionHelper.validate(n)) {
                upstream.request(n);
            }
        }

        @Override
        public void cancel() {
            // a cancellation call from downstream

            // note that upstream is non-null because by specification
            // onSubscribe must terminate before request or cancel is
            // called on the Subscription passed into onSubscribe.
            upstream.cancel();
        }

    }

}
