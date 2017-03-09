package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Action;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

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
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new DoOnEmptySubscriber<T>(child, onEmpty));
    }

    private static final class DoOnEmptySubscriber<T> implements FlowableSubscriber<T>, Subscription {

        private final Subscriber<? super T> child;
        private final Action onEmpty;
        // mutable state
        private boolean done;
        private boolean empty = true;
        private Subscription parent;

        DoOnEmptySubscriber(Subscriber<? super T> child, Action onEmpty) {
            this.child = child;
            this.onEmpty = onEmpty;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
            }
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
            child.onComplete();
        }

        @Override
        public void onNext(T t) {
            empty = false;
            child.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            if (done) {
                return;
            }
            done = true;
            child.onError(e);
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void request(long n) {
            parent.request(n);
        }
    }
}
