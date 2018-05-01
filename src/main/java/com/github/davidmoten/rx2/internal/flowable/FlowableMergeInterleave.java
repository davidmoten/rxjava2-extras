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

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableMergeInterleave<T> extends Flowable<T> {

    private final int maxConcurrent;
    private final Flowable<Flowable<T>> sources;
    private final int batchSize;

    public FlowableMergeInterleave(Flowable<Flowable<T>> sources, int maxConcurrent, int batchSize) {
        this.sources = sources;
        this.maxConcurrent = maxConcurrent;
        this.batchSize = batchSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        MergeInterleaveSubscription<T> subscription = new MergeInterleaveSubscription<T>(sources, maxConcurrent,
                batchSize, s);
        s.onSubscribe(subscription);
    }

    private static final class MergeInterleaveSubscription<T> implements Subscription, Subscriber<Flowable<T>> {

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

        // objects on queue can be Flowable, Subscriber,
        private final SimplePlainQueue<Object> queue;
        private final int batchSize;
        private final List<SourceSubscriber<T>> sourceSubscribers = new ArrayList<SourceSubscriber<T>>();
        private int sourceSubscriberIndex;
        private boolean sourcesComplete;
        private boolean allEmissionsComplete;

        public MergeInterleaveSubscription(Flowable<Flowable<T>> sources, int maxConcurrent, int batchSize,
                Subscriber<? super T> subscriber) {
            this.sources = sources;
            this.maxConcurrent = maxConcurrent;
            this.batchSize = batchSize;
            this.subscriber = subscriber;
            this.queue = new MpscLinkedQueue<Object>();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (once.compareAndSet(false, true)) {
                    sources.subscribe(this);
                }
                BackpressureHelper.produced(requested, n);
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
            s.request(maxConcurrent);
            drain();
        }

        @Override
        public void onNext(Flowable<T> f) {
            queue.offer(new SourceArrived<T>(f));
            drain();
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
                    long r = requested.get();
                    while (true) {
                        if (tryCancelled()) {
                            return;
                        }
                        // if flowable on queue then subscribe to it
                        // if flowable finished then request another
                        // if subscriber has data or terminates then emit it round-robin style
                        // subscribers should only request batchSize at a time
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
                        boolean d = finished;
                        Object o = queue.poll();
                        System.out.println("drained " + o);
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
                                handleSource((SourceArrived<T>) o);
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

        @SuppressWarnings("unchecked")
        private void handleBatchFinished(BatchFinished b) {
            SourceSubscriber<T> sub = (SourceSubscriber<T>) b;
            sub.requestMore();
        }

        private void cleanup() {
            for (SourceSubscriber<T> s : sourceSubscribers) {
                s.cancel();
            }
        }

        private void handleSource(SourceArrived<T> event) {
            SourceSubscriber<T> subscriber = new SourceSubscriber<T>(this);
            sourceSubscribers.add(subscriber);
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
            requestMore();
        }

        @Override
        public void onNext(T t) {
            count++;
            parent.queue.offer(t);
            if (count == parent.batchSize) {
                // offer batch finished indicator
                parent.queue.offer(this);
                count = 0;
            }
            parent.drain();
        }

        @Override
        public void onError(Throwable t) {
            parent.error = t;
            parent.finished = true;
            parent.drain();
        }

        @Override
        public void onComplete() {
            parent.queue.offer(new SourceComplete<T>(this));
            parent.drain();
        }
        
        void requestMore() {
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

    }

}
