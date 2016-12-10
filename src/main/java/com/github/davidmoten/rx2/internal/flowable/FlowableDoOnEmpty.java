package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Action;
import io.reactivex.internal.fuseable.ConditionalSubscriber;
import io.reactivex.internal.subscribers.BasicFuseableConditionalSubscriber;
import io.reactivex.internal.subscribers.BasicFuseableSubscriber;

/**
 * Calls a consumer just before completion if the stream was empty.
 * 
 * @param <T>
 *            the value type
 */
public final class FlowableDoOnEmpty<T> extends Flowable<T> {

    private final Publisher<T> source;
    private final Action onEmpty;

    public FlowableDoOnEmpty(Publisher<T> source, Action onEmpty) {
        Preconditions.checkNotNull(source, "source cannot be null");
        Preconditions.checkNotNull(onEmpty, "onEmpty cannot be null");
        this.source = source;
        this.onEmpty = onEmpty;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        source.subscribe(new DoOnEmptySubscriber<T>(s, onEmpty));
    }

    static final class DoOnEmptySubscriber<T> implements Subscriber<T>, Subscription {

        private final Subscriber<? super T> actual;
        private final Action onEmpty;
        //mutable state
        private boolean done;
        private boolean empty = true;
        private Subscription subscription;

        DoOnEmptySubscriber(Subscriber<? super T> actual, Action onEmpty) {
            this.actual = actual;
            this.onEmpty = onEmpty;
        }
        
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            if (empty) {
                try {
                    onEmpty.run();
                } catch (Throwable e) {
                    Exceptions.throwIfFatal(e);
                    onError(e);
                    return;
                }
            }
            done = true;
            actual.onComplete();
        }
        
        @Override
        public void onNext(T t) {
            empty = false;
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            if (done) {
                return;
            }
            done = true;
            actual.onError(e);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }

        @Override
        public void request(long n) {
            subscription.request(n);
        }
    }
}
