package com.github.davidmoten.rx2.internal.flowable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
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
    private final Publisher<? extends Publisher<? extends T>> sources;
    private final int batchSize;
    private boolean delayErrors;

    public FlowableMergeInterleave(Publisher<? extends Publisher<? extends T>> sources,
            int maxConcurrent, int batchSize, boolean delayErrors) {
        this.sources = sources;
        this.maxConcurrent = maxConcurrent;
        this.batchSize = batchSize;
        this.delayErrors = delayErrors;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        MergeInterleaveSubscription<T> subscription = new MergeInterleaveSubscription<T>(sources,
                maxConcurrent, batchSize, delayErrors, s);
        s.onSubscribe(subscription);
    }

    private static final class MergeInterleaveSubscription<T> extends AtomicInteger
            implements Subscription, Subscriber<Publisher<? extends T>> {

        private static final long serialVersionUID = -6416801556759306113L;
        private static final Object SOURCES_COMPLETE = new Object();
        private final AtomicBoolean once = new AtomicBoolean();
        private final Publisher<? extends Publisher<? extends T>> sources;
        private final int maxConcurrent;
        private final int batchSize;
        private final boolean delayErrors;
        private Subscriber<? super T> subscriber;
        private Subscription subscription;
        private volatile boolean cancelled;
        private Throwable error;
        private volatile boolean finished;
        private final AtomicLong requested = new AtomicLong();
        private long emitted;
        private final RingBuffer<BatchFinished> batchFinished;

        // objects on queue can be Flowable, Subscriber,
        private final SimplePlainQueue<Object> queue;
        private final List<SourceSubscriber<T>> sourceSubscribers = new ArrayList<SourceSubscriber<T>>();
        private boolean sourcesComplete;
        private long sourcesCount;

        public MergeInterleaveSubscription(Publisher<? extends Publisher<? extends T>> sources,
                int maxConcurrent, int batchSize, boolean delayErrors,
                Subscriber<? super T> subscriber) {
            this.sources = sources;
            this.maxConcurrent = maxConcurrent;
            this.batchSize = batchSize;
            this.delayErrors = delayErrors;
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
        public void onNext(Publisher<? extends T> f) {
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
            if (getAndIncrement() == 0) {
                int missed = 1;
                long e = emitted;
                long r = requested.get();
                while (true) {
                    if (tryCancelled()) {
                        return;
                    }
                    if (e == r) {
                        r = requested.get();
                    }
                    while (e != r) {
                        boolean d = finished;
                        if (d && !delayErrors) {
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
                                handleSourceComplete((SourceComplete<T>) o);
                            } else if (o == SOURCES_COMPLETE) {
                                handleSourcesComplete();
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
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        private void handleSourcesComplete() {
            sourcesComplete = true;
            if (sourceSubscribers.isEmpty()) {
                finished = true;
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
            queue.offer(subscriber);
            event.flowable.subscribe(subscriber);
        }

        private void handleSourceComplete(SourceComplete<T> event) {
            sourceSubscribers.remove(event.subscriber);
            if (!sourcesComplete) {
                subscription.request(1);
            } else if (sourceSubscribers.isEmpty() && sourcesComplete) {
                finished = true;
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
        final Publisher<? extends T> flowable;

        SourceArrived(Publisher<? extends T> flowable) {
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
