package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public final class FlowableSwitchMapSliding<T> extends Flowable<T> {

    private final Flowable<Flowable<T>> sources;
    private final int size;
    private final int requestBatchSize;

    public FlowableSwitchMapSliding(Flowable<Flowable<T>> sources, int size, int requestBatchSize) {
        this.sources = sources;
        this.size = size;
        this.requestBatchSize = requestBatchSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        sources.subscribe(new SourcesSubscriber<T>(size, requestBatchSize, child));
    }

    @SuppressWarnings("serial")
    private static final class SourcesSubscriber<T> extends AtomicInteger
            implements Subscriber<Flowable<T>>, Subscription {

        private final int size;
        private final int requestBatchSize;
        private final Subscriber<? super T> child;
        private final SimplePlainQueue<T> queue;
        private final AtomicLong requested = new AtomicLong();

        private Subscription parent;
        private int nextIndex;
        private SourceSubscriber<T>[] subscribers;
        private int count;
        private volatile boolean cancelled;
        private volatile Throwable error;
        volatile boolean done;

        @SuppressWarnings("unchecked")
        SourcesSubscriber(int size, int requestBatchSize, Subscriber<? super T> child) {
            this.size = size;
            this.requestBatchSize = requestBatchSize;
            this.child = child;
            this.subscribers = new SourceSubscriber[requestBatchSize + 1];
            this.queue = new MpscLinkedQueue<T>();
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
                parent.request(size);
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void onNext(Flowable<T> source) {
            boolean firstGroup;
            if (count < size) {
                count++;
                firstGroup = true;
            } else
                firstGroup = false;
            SourceSubscriber<T> s = new SourceSubscriber<T>(queue, this, requestBatchSize,
                    nextIndex);
            subscribers[nextIndex] = s;
            final int idx = nextIndex;
            nextIndex = (nextIndex + 1) % (size);
            Flowable<T> source2;
            if (!firstGroup) {
                // TODO make static class for consumer
                source2 = source.doOnNext(new Consumer<T>() {
                    boolean first = true;

                    @Override
                    public void accept(T x) throws Exception {
                        if (first) {
                            int i = (idx + 1) % size;
                            subscribers[i].cancel();
                            subscribers[i] = null;
                            parent.request(1);
                        }
                        first = false;
                    }
                });
            } else {
                source2 = source;
            }
            source2.subscribe(s);
        }

        @Override
        public void onError(Throwable t) {
            parent.cancel();
            child.onError(t);
        }

        @Override
        public void onComplete() {
            done = true;
        }

        void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (done) {
                                Throwable err = error;
                                if (err != null) {
                                    queue.clear();
                                    child.onError(err);
                                    return;
                                } else {
                                    boolean finished = true;
                                    for (int i = 0; i < subscribers.length; i++) {
                                        if (subscribers[i] != null) {
                                            finished = false;
                                        }
                                    }
                                    if (finished) {
                                        child.onComplete();
                                        return;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (e > 0 && r != Long.MAX_VALUE) {
                        requested.addAndGet(-e);
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        void removeSubscriber(int subscriberIndex) {
            subscribers[subscriberIndex] = null;
        }

    }

    private static final class SourceSubscriber<T> implements Subscriber<T>, Subscription {

        private final SimplePlainQueue<T> queue;
        private final int requestBatchSize;
        private final SourcesSubscriber<T> sources;
        private final int subscriberIndex;

        private Subscription parent;
        private int count;

        public SourceSubscriber(SimplePlainQueue<T> queue, SourcesSubscriber<T> sources,
                int requestBatchSize, int subscriberIndex) {
            this.queue = queue;
            this.sources = sources;
            this.requestBatchSize = requestBatchSize;
            this.subscriberIndex = subscriberIndex;
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.parent = s;
            s.request(requestBatchSize);
        }

        @Override
        public void onNext(T t) {
            count = (count + 1) % requestBatchSize;
            queue.offer(t);
            if (count == 0) {
                parent.request(requestBatchSize);
            }
            sources.drain();
        }

        @Override
        public void onError(Throwable e) {
            parent.cancel();
            sources.error = e;
            sources.done = true;
            sources.drain();
        }

        @Override
        public void onComplete() {
            cancel();
            sources.removeSubscriber(subscriberIndex);
            sources.drain();
        }

        @Override
        public void request(long n) {
            parent.request(n);
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

    }

}
