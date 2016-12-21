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
    private final String searchFor;
    private final int bufferSize;

    public FlowableStringSplit(Flowable<String> source, String searchFor, int bufferSize) {
        this.source = source;
        this.searchFor = searchFor;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super String> s) {
        source.subscribe(new StringSplitSubscriber(s, searchFor, bufferSize));
    }

    @SuppressWarnings("serial")
    private static final class StringSplitSubscriber extends AtomicLong
            implements Subscriber<String>, Subscription {

        private final Subscriber<? super String> actual;
        private final String searchFor;
        private final int bufferSize;
        // queue of notifications
        private final Queue<Object> queue = new ConcurrentLinkedQueue<Object>();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicBoolean once = new AtomicBoolean();

        private StringBuilder leftOver;
        private int index;
        private int searchIndex;
        private Subscription parent;

        StringSplitSubscriber(Subscriber<? super String> actual, String searchFor, int bufferSize) {
            this.actual = actual;
            this.searchFor = searchFor;
            this.bufferSize = bufferSize;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.parent = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(this, n);
                if (once.compareAndSet(false, true)) {
                    if (n == Long.MAX_VALUE) {
                        parent.request(Long.MAX_VALUE);
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
            if (wip.getAndIncrement() != 0) {
                return;
            }
            int missed = wip.get();
            long r = get(); // requested
            while (true) {
                long e = 0; // emitted
                while (e < r) {
                    if (find()) {
                        e++;
                    } else {
                        Object o = queue.poll();
                        if (o == null) {
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
                                leftOver = new StringBuilder(bufferSize);
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
            if (leftOver == null) {
                return false;
            } else {
                int i = leftOver.indexOf(searchFor, searchIndex);
                if (i != -1) {
                    // emit and adjust indexes
                    String s = leftOver.substring(searchIndex, i);
                    searchIndex = i + searchFor.length();
                    if (searchIndex > bufferSize) {
                        // shrink leftOver
                        leftOver.delete(0, searchIndex);
                        index = 0;
                        searchIndex = 0;
                    } else {
                        index = searchIndex;
                    }
                    actual.onNext(s);
                    return true;
                } else {
                    // emit nothing but adjust searchIndex to the right
                    searchIndex = Math.max(searchIndex, leftOver.length() - searchFor.length() - 1);
                    return false;
                }
            }
        }
    }

}
