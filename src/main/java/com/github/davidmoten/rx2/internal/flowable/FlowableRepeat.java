package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableRepeat<T> extends Flowable<T> {

    private final T value;
    private final long count;

    public FlowableRepeat(T value, long count) {
        this.value = value;
        this.count = count;
    }

    @Override
    protected void subscribeActual(org.reactivestreams.Subscriber<? super T> child) {
        RepeatSubscription<T> sub = new RepeatSubscription<T>(child, value, count);
        child.onSubscribe(sub);
    }

    @SuppressWarnings("serial")
    private static class RepeatSubscription<T> extends AtomicLong implements Subscription {

        private final Subscriber<? super T> child;
        private final T value;
        private final long count;

        private volatile boolean cancelled;
        private long counter;

        public RepeatSubscription(Subscriber<? super T> child, T value, long count) {
            this.child = child;
            this.value = value;
            this.count = count;
            this.counter = count;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.add(this, n) == 0) {
                    long requested = n;
                    long emitted = 0;
                    do {
                        emitted = requested;
                        while (requested-- > 0 && !cancelled && (count == -1 || counter-- > 0)) {
                            child.onNext(value);
                        }
                    } while ((requested = this.addAndGet(-emitted)) > 0);
                    if (count >= 0 && !cancelled) {
                        child.onComplete();
                    }
                }
            }
        }

        @Override
        public void cancel() {
            this.cancelled = true;
        }

    }

}
