package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

public class FlowablePassThroughTest {

    @Test
    public void test() {
        new FlowablePassThrough<Integer>(Flowable.just(1, 2, 3)) //
                .test() //
                .assertValues(1, 2, 3) //
                .assertComplete();
    }

    @Test
    public void testSubscriber() {
        final List<Integer> list = new ArrayList<Integer>();
        new FlowablePassThrough<Integer>(Flowable.just(1, 2, 3)) //
                .subscribe(new Subscriber<Integer>() {

                    private Subscription s;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.s = s;
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Integer t) {
                        list.add(t);
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {

                    }
                });
        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    static final class FlowablePassThrough<T> extends Flowable<T> {

        private final Flowable<T> source;

        public FlowablePassThrough(Flowable<T> source) {
            this.source = source;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            source.subscribe(new PassThroughSubscriber<T>(child));
        }

        @SuppressWarnings("serial")
        private static final class PassThroughSubscriber<T> extends AtomicLong
                implements Subscriber<T>, Subscription {

            private final Subscriber<? super T> child;
            private AtomicReference<Subscription> parent = new AtomicReference<Subscription>();

            public PassThroughSubscriber(Subscriber<? super T> child) {
                this.child = child;
                child.onSubscribe(this);
            }

            @Override
            public void onSubscribe(Subscription parent) {
                SubscriptionHelper.deferredSetOnce(this.parent, this, parent);
            }

            @Override
            public void request(long n) {
                SubscriptionHelper.deferredRequest(parent, this, n);
            }

            @Override
            public void cancel() {
                SubscriptionHelper.cancel(parent);
            }

            @Override
            public void onNext(T t) {
                System.out.println(t);
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

        }

    }

}
