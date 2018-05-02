package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableDeepTransform<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final BiFunction<Flowable<T>, AtomicBoolean, Flowable<T>> transform;

    public FlowableDeepTransform(Flowable<T> source, BiFunction<Flowable<T>, AtomicBoolean, Flowable<T>> transform) {
        this.source = source;
        this.transform = transform;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        DeepTransformSubscriber<T> subscription = new DeepTransformSubscriber<T>(transform, s);
        s.onSubscribe(subscription);
        source.subscribe(subscription);
    }

    private static final class DeepTransformSubscriber<T> implements Subscription, Subscriber<T> {

        private final BiFunction<Flowable<T>, AtomicBoolean, Flowable<T>> transform;
        private final Subscriber<? super T> child;
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private Subscription parent;

        public DeepTransformSubscriber(BiFunction<Flowable<T>, AtomicBoolean, Flowable<T>> transform,
                Subscriber<? super T> child) {
            this.transform = transform;
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.parent = s;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
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
        public void cancel() {
            // TODO Auto-generated method stub

        }
        
        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    break;
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
        
    }

}
