package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableStringSplitSimple extends Flowable<String> {

    private final Flowable<String> source;
    private final String delimiter;

    public FlowableStringSplitSimple(Flowable<String> source, String delimiter) {
        Preconditions.checkNotNull(source);
        Preconditions.checkNotNull(delimiter);
        Preconditions.checkArgument(delimiter.length() > 0);
        this.source = source;
        this.delimiter = delimiter;
    }

    @Override
    protected void subscribeActual(Subscriber<? super String> s) {
        source.subscribe(new StringSplitSubscriber(s, delimiter));
    }

    @SuppressWarnings("serial")
    private static final class StringSplitSubscriber extends AtomicLong
            implements Subscriber<String>, Subscription {

        private final Subscriber<? super String> actual;
        // queue of notifications
        private final transient SimplePlainQueue<String> queue = new SpscLinkedArrayQueue<String>(
                16);
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicBoolean once = new AtomicBoolean();

        private final DelimitedStringLinkedList ss;
        private volatile boolean cancelled;
        private Subscription parent;
        private boolean unbounded;

        private Throwable error;
        private volatile boolean done;

        StringSplitSubscriber(Subscriber<? super String> actual, String delimiter) {
            this.actual = actual;
            this.ss = new DelimitedStringLinkedList(delimiter);
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.parent = subscription;
            actual.onSubscribe(this);
        }

        @Override
        public void cancel() {
            cancelled = true;
            parent.cancel();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(this, n);
                if (once.compareAndSet(false, true)) {
                    if (n == Long.MAX_VALUE) {
                        parent.request(Long.MAX_VALUE);
                        unbounded = true;
                    } else {
                        parent.request(1);
                    }
                }
                drain();
            }
        }

        @Override
        public void onNext(String t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        @Override
        public void onError(Throwable e) {
            error = e;
            done = true;
            drain();
        }

        private void drain() {
            if (wip.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            while (true) {
                long r = get(); // requested
                long e = 0; // emitted
                while (e != r) {
                    if (cancelled) {
                        return;
                    }
                    if (find()) {
                        e++;
                    } else {
                        // must read `done` before poll occurs
                        boolean d = done;
                        String t = queue.poll();
                        if (t == null) {
                            if (d) {
                                Throwable err = error;
                                if (err != null) {
                                    ss.clear();
                                    actual.onError(err);
                                    return;
                                } else {
                                    String remaining = ss.remaining();
                                    final boolean checkCancelled;
                                    if (remaining != null) {
                                        ss.clear();
                                        queue.clear();
                                        actual.onNext(remaining);
                                        e++;
                                        checkCancelled = true;
                                    } else if (ss.addCalled()) {
                                        ss.clear();
                                        queue.clear();
                                        actual.onNext("");
                                        e++;
                                        checkCancelled = true;
                                    } else {
                                        checkCancelled = false;
                                    }
                                    if (!checkCancelled || !cancelled) {
                                        actual.onComplete();
                                    }
                                    return;
                                }
                            } else if (!unbounded) {
                                parent.request(1);
                            }
                            break;
                        } else {
                            ss.add(t);
                        }
                    }
                }
                if (e > 0) {
                    BackpressureHelper.produced(this, e);
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
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
            if (ss == null) {
                return false;
            }
            String s = ss.next();
            if (s != null) {
                actual.onNext(s);
                return true;
            } else {
                return false;
            }
        }
    }

}
