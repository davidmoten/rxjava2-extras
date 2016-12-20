package com.github.davidmoten.rx2.internal.flowable;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.internal.util.NotificationLite;

public final class FlowableStringSplit extends Flowable<String> {

    private final Flowable<String> source;
    private final String token;
    private final int bufferSize;

    public FlowableStringSplit(Flowable<String> source, String token, int bufferSize) {
        this.source = source;
        this.token = token;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super String> s) {
        source.subscribe(new StringSplitSubscriber(s, token, bufferSize));
    }

    @SuppressWarnings("serial")
    private static final class StringSplitSubscriber extends AtomicLong
            implements Subscriber<String>, Subscription {

        private final Subscriber<? super String> actual;
        private final String token;
        private final int bufferSize;
        // queue of notifications
        private final Queue<Object> queue = new ConcurrentLinkedQueue<Object>();
        private final AtomicInteger wip = new AtomicInteger();

        private StringBuilder leftOver;
        private int index;
        private int searchIndex;
        private Subscription parent;
        private final AtomicBoolean once = new AtomicBoolean();

        StringSplitSubscriber(Subscriber<? super String> actual, String token, int bufferSize) {
            this.actual = actual;
            this.token = token;
            this.bufferSize = bufferSize;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.parent = subscription;
            actual.onSubscribe(this);
            System.out.println("subscribed");
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void request(long n) {
            System.out.println("requested " + n);
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(this, n);
                if (once.compareAndSet(false, true)) {
                    if (n == Long.MAX_VALUE) {
                        parent.request(n);
                    } else {
                        parent.request(1);
                    }
                }
                drain();
            } else {
                throw new IllegalArgumentException("illegal request amount: " + n);
            }
        }

        @Override
        public void onNext(String t) {
            System.out.println("onNext=" + t);
            queue.add(NotificationLite.next(t));
            drain();
        }

        @Override
        public void onComplete() {
            queue.offer(NotificationLite.complete());
            drain();
        }

        @Override
        public void onError(Throwable e) {
            queue.offer(NotificationLite.error(e));
            drain();
        }

        private void drain() {
            System.out.println("drain");
            if (wip.getAndIncrement() != 0) {
                return;
            }
            int missed = wip.get();
            long r = get(); // requested
            while (true) {
                long e = 0; // emitted
                while (e < r) {
                    boolean found = false;
                    if (leftOver != null) {
                        found = find();
                        if (found) {
                            e++;
                        }
                    }
                    if (!found) {
                        Object o = queue.poll();
                        System.out.println("polled " + o);
                        if (o == null) {
                            System.out.println("requesting 1");
                            parent.request(1);
                            if (wip.addAndGet(-missed) == 0) {
                                return;
                            }
                        } else if (NotificationLite.isComplete(o)) {
                            if (leftOver != null) {
                                String s = leftOver.substring(index, leftOver.length());
                                leftOver = null;
                                queue.clear();
                                actual.onNext(s.toString());
                                e++;
                            }
                            actual.onComplete();
                            return;
                        } else if (NotificationLite.isError(o)) {
                            leftOver = null;
                            queue.clear();
                            actual.onError(NotificationLite.getError(o));
                            return;
                        } else {
                            if (leftOver == null) {
                                leftOver = new StringBuilder();
                            }
                            leftOver.append((String) o);
                        }
                    }
                }
                if (e > 0) {
                    r = BackpressureHelper.produced(this, e);
                    if (r == 0 && wip.addAndGet(-missed) == 0) {
                        return;
                    }
                } else if (wip.addAndGet(-missed) == 0) {
                    return;
                }
            }
        }

        /**
         * Returns true if and only if a value emitted.
         * 
         * @return true if and only if a value emitted
         */
        private boolean find() {
            boolean found = false;
            // brute force search is efficient generally for a single pass and
            // short token
            int i;
            for (i = searchIndex; i < leftOver.length() - token.length(); i++) {
                int j = 0;
                while (j < token.length() && leftOver.charAt(i + j) == token.charAt(j)) {
                    j++;
                }
                if (j == token.length()) {
                    found = true;
                    break;
                }
            }
            if (found) {
                // emit and adjust indexes
                String s = leftOver.substring(searchIndex, i);
                searchIndex = i + token.length();
                index = searchIndex;
                if (index == leftOver.length()) {
                    leftOver = null;
                    index = 0;
                    searchIndex = 0;
                } else if (index > bufferSize) {
                    // shrink leftOver
                    leftOver.delete(0, index);
                    index = 0;
                    searchIndex = 0;
                }
                System.out.println("emitting " + s);
                actual.onNext(s);
                return true;
            } else {
                // emit nothing but adjust searchIndex to the right
                searchIndex = Math.max(searchIndex, leftOver.length() - token.length() - 1);
                return false;
            }
        }

    }

}
