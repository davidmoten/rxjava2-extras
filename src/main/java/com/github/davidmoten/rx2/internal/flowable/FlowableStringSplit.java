package com.github.davidmoten.rx2.internal.flowable;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.internal.util.NotificationLite;

public final class FlowableStringSplit extends Flowable<String> {

    private final Flowable<String> source;
    private final Pattern pattern;
    private final int bufferSize;

    public FlowableStringSplit(Flowable<String> source, Pattern pattern, int bufferSize) {
        this.source = source;
        this.pattern = pattern;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super String> s) {
        source.subscribe(new StringSplitSubscriber(s, pattern, bufferSize));
    }

    @SuppressWarnings("serial")
    private static final class StringSplitSubscriber extends AtomicLong
            implements Subscriber<String>, Subscription {

        private final Subscriber<? super String> actual;
        private final Pattern pattern;
        private final int bufferSize;
        // queue of notifications
        private final Queue<Object> queue = new ConcurrentLinkedQueue<Object>();
        private final AtomicInteger wip = new AtomicInteger();

        private StringBuilder leftOver;
        private int index;
        private Subscription subscription;

        StringSplitSubscriber(Subscriber<? super String> actual, Pattern pattern, int bufferSize) {
            this.actual = actual;
            this.pattern = pattern;
            this.bufferSize = bufferSize;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(this, n);
                drain();
            } else {
                throw new IllegalArgumentException("illegal request amount: " + n);
            }
        }

        @Override
        public void onNext(String t) {
            // TODO fast path when leftOver is null
            if (leftOver == null) {
                leftOver = new StringBuilder(bufferSize);
                index = 0;
            }
            leftOver.append(t);
            drain();
        }

        @Override
        public void onComplete() {
            if (leftOver != null) {
                String s = leftOver.toString();
                leftOver = null;
                queue.offer(s);
            }
            queue.offer(NotificationLite.complete());
            drain();
        }

        @Override
        public void onError(Throwable e) {
            queue.offer(NotificationLite.error(e));
            drain();
        }

        private void drain() {
            if (wip.getAndIncrement() == 0) {
                return;
            }
            while (true) {
                long r = get(); // requested
                long e = 0; // emitted
                while (e != r) {
                    // TODO
                    Matcher matcher = pattern.matcher(leftOver.subSequence(index, leftOver.length()));
                    if (matcher.find()) {
                        String s = leftOver.substring(0, matcher.start());
                        index = matcher.end();
                        if (index >= bufferSize) {
                            // shrink the buffer once we reach bufferSize chars
                            leftOver.delete(0, index);
                        }
                        queue.offer(NotificationLite.next(s));
                        drain();
                    } else {
                        request(1);
                    }
                }
            }
        }

    }

}
