package com.github.davidmoten.rx2.internal.flowable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.util.RingBuffer;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMergeInterleave<T> extends Flowable<T> {

    private final int maxConcurrent;
    private final Flowable<Flowable<T>> sources;
    private final int batchSize;
    private boolean delayError;

    public FlowableMergeInterleave(Flowable<Flowable<T>> sources, int maxConcurrent, int batchSize,
            boolean delayError) {
        this.sources = sources;
        this.maxConcurrent = maxConcurrent;
        this.batchSize = batchSize;
        this.delayError = delayError;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        MergeInterleaveSubscription<T> subscription = new MergeInterleaveSubscription<T>(sources,
                maxConcurrent, batchSize, delayError, s);
        s.onSubscribe(subscription);
    }

    private static final class MergeInterleaveSubscription<T>
            implements Subscription, Subscriber<Flowable<T>> {

        private static final Object SOURCES_COMPLETE = new Object();
        private final AtomicBoolean once = new AtomicBoolean();
        private final Flowable<Flowable<T>> sources;
        private final int maxConcurrent;
        private final int batchSize;
        private final boolean delayError;
        private Subscriber<? super T> subscriber;
        private Subscription subscription;
        private volatile boolean cancelled;
        private Throwable error;
        private volatile boolean finished;
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private long emitted;
        private final RingBuffer<BatchFinished> batchFinished;

        // objects on queue can be Flowable, Subscriber,
        private final SimplePlainQueue<Object> queue;
        private final List<SourceSubscriber<T>> sourceSubscribers = new ArrayList<SourceSubscriber<T>>();
        private boolean sourcesComplete;
        private int sourcesCount;

        public MergeInterleaveSubscription(Flowable<Flowable<T>> sources, int maxConcurrent,
                int batchSize, boolean delayError, Subscriber<? super T> subscriber) {
            this.sources = sources;
            this.maxConcurrent = maxConcurrent;
            this.batchSize = batchSize;
            this.delayError = delayError;
            this.subscriber = subscriber;
            this.queue = new MpscLinkedQueue<Object>();
            this.batchFinished = RingBuffer.create(maxConcurrent + 1);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                if (once.compareAndSet(false, true)) {
                    sources.subscribe(this);
                    subscription.request(maxConcurrent);
                }
                drain();
            }
        }

        @Override
        public void cancel() {
            this.cancelled = true;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.subscription = s;
            drain();
        }

        @Override
        public void onNext(Flowable<T> f) {
            sourcesCount++;
            queue.offer(new SourceArrived<T>(f));
            if (sourcesCount >= maxConcurrent) {
                drain();
            }
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            finished = true;
            drain();
        }

        @Override
        public void onComplete() {
            queue.offer(SOURCES_COMPLETE);
            drain();
        }

        private boolean tryCancelled() {
            if (cancelled) {
                subscription.cancel();
                queue.clear();
                return true;
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                long e = emitted;
                while (true) {
                    if (tryCancelled()) {
                        return;
                    }
                    long r = requested.get();
                    while (e != r) {
                        boolean d = finished;
                        if (d && !delayError) {
                            Throwable err = error;
                            if (err != null) {
                                error = null;
                                cleanup();
                                subscriber.onError(err);
                                return;
                            }
                        }
                        Object o = queue.poll();
                        if (o == null) {
                            if (d) {
                                Throwable err = error;
                                if (err != null) {
                                    error = null;
                                    cleanup();
                                    subscriber.onError(err);
                                } else {
                                    subscriber.onComplete();
                                }
                                return;
                            } else {
                                break;
                            }
                        } else {
                            if (o instanceof BatchFinished) {
                                handleBatchFinished((BatchFinished) o);
                            } else if (o instanceof SourceArrived) {
                                handleSourceArrived((SourceArrived<T>) o);
                            } else if (o instanceof SourceComplete) {
                                handleSourceTerminated((SourceComplete<T>) o);
                            } else if (o == SOURCES_COMPLETE) {
                                sourcesComplete = true;
                            } else {
                                subscriber.onNext((T) o);
                                e++;
                            }
                        }
                        if (tryCancelled()) {
                            return;
                        }
                    }
                    emitted = e;
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        private void handleBatchFinished(BatchFinished b) {
            Preconditions.checkNotNull(b);
            boolean ok = batchFinished.offer(b);
            if (!ok) {
                throw new RuntimeException("ring buffer full!");
            }
            System.out.println("batchFinished=" + batchFinished);
            while (true) {
                BatchFinished s = batchFinished.poll();
                if (s != null) {
                    s.requestMore();
                } else {
                    break;
                }
            }
        }

        private void cleanup() {
            for (SourceSubscriber<T> s : sourceSubscribers) {
                s.cancel();
            }
        }

        private void handleSourceArrived(SourceArrived<T> event) {
            SourceSubscriber<T> subscriber = new SourceSubscriber<T>(this);
            sourceSubscribers.add(subscriber);
            queue.offer(subscriber);
            event.flowable.subscribe(subscriber);
        }

        private void handleSourceTerminated(SourceComplete<T> event) {
            sourceSubscribers.remove(event.subscriber);
            if (!sourcesComplete) {
                subscription.request(1);
            } else if (sourceSubscribers.isEmpty()) {
                subscriber.onComplete();
            }
        }

        public void sourceError(Throwable t) {
            error = t;
            finished = true;
            drain();
        }

        public void sourceComplete(SourceSubscriber<T> sourceSubscriber) {
            queue.offer(new SourceComplete<T>(sourceSubscriber));
            drain();
        }

        public void sourceNext(T t, SourceSubscriber<T> sourceSubscriber) {
            queue.offer(t);
            System.out.println("value on queue " + t);
            if (sourceSubscriber != null) {
                queue.offer(sourceSubscriber);
                System.out.println(sourceSubscriber + " batch finished on queue");
            }
            drain();
        }
    }

    private final static class SourceSubscriber<T> implements Subscriber<T>, BatchFinished {

        private final MergeInterleaveSubscription<T> parent;
        private AtomicReference<Subscription> subscription = new AtomicReference<Subscription>();
        private int count = 0;

        SourceSubscriber(MergeInterleaveSubscription<T> parent) {
            this.parent = parent;
        }

        @Override
        public String toString() {
            return "SourceSubscriber-" + hashCode();
        }

        @Override
        public void onSubscribe(Subscription s) {
            SubscriptionHelper.setOnce(subscription, s);
        }

        @Override
        public void onNext(T t) {
            count++;
            boolean batchFinished = count == parent.batchSize;
            if (batchFinished) {
                count = 0;
            }
            parent.sourceNext(t, batchFinished ? this : null);
        }

        @Override
        public void onError(Throwable t) {
            parent.sourceError(t);
        }

        @Override
        public void onComplete() {
            System.out.println("COMPLETE");
            parent.sourceComplete(this);
        }

        @Override
        public void requestMore() {
            System.out.println(this + " requesting more ");
            subscription.get().request(parent.batchSize);
        }

        void cancel() {
            while (true) {
                Subscription s = subscription.get();
                if (subscription.compareAndSet(s, SubscriptionHelper.CANCELLED)) {
                    s.cancel();
                    break;
                }
            }
        }
    }

    private static final class SourceArrived<T> {
        final Flowable<T> flowable;

        SourceArrived(Flowable<T> flowable) {
            this.flowable = flowable;
        }
    }

    private static final class SourceComplete<T> {
        final Subscriber<T> subscriber;

        SourceComplete(Subscriber<T> subscriber) {
            this.subscriber = subscriber;
        }
    }

    private interface BatchFinished {
        void requestMore();
    }

}
