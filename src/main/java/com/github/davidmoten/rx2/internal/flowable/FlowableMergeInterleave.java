package com.github.davidmoten.rx2.internal.flowable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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

    public FlowableMergeInterleave(Flowable<Flowable<T>> sources, int maxConcurrent,
            int batchSize) {
        this.sources = sources;
        this.maxConcurrent = maxConcurrent;
        this.batchSize = batchSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        MergeInterleaveSubscription<T> subscription = new MergeInterleaveSubscription<T>(sources,
                maxConcurrent, batchSize, s);
        s.onSubscribe(subscription);
    }

    private static final class MergeInterleaveSubscription<T>
            implements Subscription, Subscriber<Flowable<T>> {

        private static final Object SOURCES_COMPLETE = new Object();
        private final AtomicBoolean once = new AtomicBoolean();
        private final Flowable<Flowable<T>> sources;
        private final int maxConcurrent;
        private Subscriber<? super T> subscriber;
        private Subscription subscription;
        private volatile boolean cancelled;
        private Throwable error;
        private volatile boolean finished;
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final Queue<T> emissions = new LinkedList<T>();
        private long emitted;
        private final RingBuffer<BatchFinished> batchFinished;

        // objects on queue can be Flowable, Subscriber,
        private final SimplePlainQueue<Object> queue;
        private final int batchSize;
        private final List<SourceSubscriber<T>> sourceSubscribers = new ArrayList<SourceSubscriber<T>>();
        private int sourceSubscriberIndex;
        private boolean sourcesComplete;
        private boolean allEmissionsComplete;
        private int sourcesCount;
        private boolean isFirst = true;

        public MergeInterleaveSubscription(Flowable<Flowable<T>> sources, int maxConcurrent,
                int batchSize, Subscriber<? super T> subscriber) {
            this.sources = sources;
            this.maxConcurrent = maxConcurrent;
            this.batchSize = batchSize;
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
                emissions.clear();
                return true;
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    while (true) {
                        if (tryCancelled()) {
                            return;
                        }
                        long r = requested.get();
                        long e = emitted;
                        while (e != r) {
                            T t = emissions.poll();
                            if (t != null) {
                                subscriber.onNext(t);
                                e++;
                            } else {
                                if (allEmissionsComplete) {
                                    subscriber.onComplete();
                                    return;
                                } else {
                                    break;
                                }
                            }
                            if (tryCancelled()) {
                                return;
                            }
                        }
                        emitted = e;
                        if (e == r) {
                            break;
                        }
                        boolean d = finished;
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
                                // is emission
                                emissions.offer((T) o);
                            }
                        }
                        if (tryCancelled()) {
                            return;
                        }
                    }
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
            batchFinished.poll().requestMore();
        }

        private void cleanup() {
            for (SourceSubscriber<T> s : sourceSubscribers) {
                s.cancel();
            }
        }

        private void handleSourceArrived(SourceArrived<T> event) {
            SourceSubscriber<T> subscriber = new SourceSubscriber<T>(this);
            sourceSubscribers.add(subscriber);
            batchFinished.offer(subscriber);
            if (isFirst) {
                queue.offer(subscriber);
                isFirst = false;
            }
            event.flowable.subscribe(subscriber);
        }

        private void handleSourceTerminated(SourceComplete<T> event) {
            int i = sourceSubscribers.indexOf(event.subscriber);
            if (i >= sourceSubscriberIndex) {
                sourceSubscriberIndex--;
            }
            sourceSubscribers.remove(event.subscriber);
            if (!sourcesComplete) {
                subscription.request(1);
            } else if (sourceSubscribers.isEmpty()) {
                allEmissionsComplete = true;
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
            if (sourceSubscriber != null) {
                queue.offer(sourceSubscriber);
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
            parent.sourceComplete(this);
        }

        @Override
        public void requestMore() {
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
