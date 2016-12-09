package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
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
        Preconditions.checkNotNull(source);
        this.source = source;
        this.onEmpty = onEmpty;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        if (s instanceof ConditionalSubscriber) {
            source.subscribe(new DoOnEmptyConditionalSubscriber<T>(
                    (ConditionalSubscriber<? super T>) s, onEmpty));
        } else {
            source.subscribe(new DoOnEmptySubscriber<T>(s, onEmpty));
        }
    }

    static final class DoOnEmptySubscriber<T> extends BasicFuseableSubscriber<T, T> {

        final Action onEmpty;
        boolean empty = true;

        DoOnEmptySubscriber(Subscriber<? super T> actual, Action onEmpty) {
            super(actual);
            this.onEmpty = onEmpty;
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
                    fail(e);
                    return;
                }
            }
            done = true;
            actual.onComplete();
        }

        @Override
        public int requestFusion(int mode) {
            return transitiveBoundaryFusion(mode);
        }

        @Override
        public T poll() throws Exception {
            T v = qs.poll();
            if (v != null) {
                empty = false;
            }
            return v;
        }

        @Override
        public void onNext(T t) {
            empty = false;
            actual.onNext(t);
        }
    }

    static final class DoOnEmptyConditionalSubscriber<T>
            extends BasicFuseableConditionalSubscriber<T, T> {

        private final Action onEmpty;
        private boolean empty = true;

        DoOnEmptyConditionalSubscriber(ConditionalSubscriber<? super T> actual, Action onEmpty) {
            super(actual);
            this.onEmpty = onEmpty;
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
                    fail(e);
                    return;
                }
            }
            done = true;
            actual.onComplete();
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            empty = false;
            actual.onNext(t);
        }

        @Override
        public boolean tryOnNext(T t) {
            empty = false;
            return actual.tryOnNext(t);
        }

        @Override
        public int requestFusion(int mode) {
            return transitiveBoundaryFusion(mode);
        }

        @Override
        public T poll() throws Exception {
            T v = qs.poll();
            if (v != null) {
                empty = false;
            }
            return v;
        }
    }
}
