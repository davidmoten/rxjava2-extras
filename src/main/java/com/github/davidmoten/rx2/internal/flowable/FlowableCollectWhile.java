package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.internal.fuseable.ConditionalSubscriber;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableCollectWhile<T, R> extends Flowable<R> {

    private final Flowable<T> source;
    private final Callable<R> collectionFactory;
    private final BiFunction<? super R, ? super T, ? extends R> add;
    private final BiPredicate<? super R, ? super T> condition;
    private final boolean emitRemainder;

    public FlowableCollectWhile(Flowable<T> source, Callable<R> collectionFactory,
            BiFunction<? super R, ? super T, ? extends R> add,
            BiPredicate<? super R, ? super T> condition, boolean emitRemainder) {
        super();
        this.source = source;
        this.collectionFactory = collectionFactory;
        this.add = add;
        this.condition = condition;
        this.emitRemainder = emitRemainder;
    }

    @Override
    protected void subscribeActual(Subscriber<? super R> child) {
        CollectWhileSubscriber<T, R> subscriber = new CollectWhileSubscriber<T, R>(
                collectionFactory, add, condition, child, emitRemainder);
        source.subscribe(subscriber);
    }

    @SuppressWarnings("serial")
    private static final class CollectWhileSubscriber<T, R> extends AtomicInteger
            implements Subscriber<T>, Subscription, ConditionalSubscriber<T> {

        private final Callable<R> collectionFactory;
        private final BiFunction<? super R, ? super T, ? extends R> add;
        private final BiPredicate<? super R, ? super T> condition;
        private final Subscriber<? super R> child;
        private final boolean emitRemainder;
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<R> queue = new SpscLinkedArrayQueue<R>(16);

        private Subscription parent;
        private R collection;
        private volatile boolean done;
        private Throwable error; // does not need to be volatile because is set
                                 // before `done` and read after `done`

        private volatile boolean cancelled;

        CollectWhileSubscriber(Callable<R> collectionFactory,
                BiFunction<? super R, ? super T, ? extends R> add,
                BiPredicate<? super R, ? super T> condition, Subscriber<? super R> child,
                boolean emitRemainder) {
            this.collectionFactory = collectionFactory;
            this.add = add;
            this.condition = condition;
            this.child = child;
            this.emitRemainder = emitRemainder;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            // this path taken by upstream if not enabled to call `tryOnNext`
            if (!tryOnNext(t)) {
                parent.request(1);
            }
        }

        @Override
        public boolean tryOnNext(T t) {
            if (done) {
                return true;
            }
            if (collection == null && !collectionCreated()) {
                return true;
            }
            boolean collect;
            try {
                collect = condition.test(collection, t);
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return true;
            }
            if (!collect) {
                queue.offer(collection);
                if (!collectionCreated()) {
                    return true;
                }
            }
            try {
                collection = add.apply(collection, t);
                if (collection == null) {
                    throw new NullPointerException("add function should not return null");
                }
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return true;
            }
            drain();
            return !collect;
        }

        public boolean collectionCreated() {
            try {
                collection = collectionFactory.call();
                if (collection == null) {
                    throw new NullPointerException("collectionFactory should not return null");
                }
                return true;
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return false;
            }
        }

        @Override
        public void onError(Throwable e) {
            if (done) {
                RxJavaPlugins.onError(e);
                return;
            }
            // must set `error` before done because `error` is not volatile and
            // `done` is
            error = e;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            R col = collection;
            if (col != null) {
                collection = null;
                // ensure that the remainder is emitted
                // if configured to
                if (emitRemainder) {
                    queue.offer(col);
                }
            }
            done = true;
            drain();
        }

        private void drain() {
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
                        //must read `done` before polling queue
                        boolean d = done;
                        R c = queue.poll();
                        if (c == null) {
                            if (d) {
                                // `error` must be read AFTER `done` for
                                // full visibility
                                Throwable err = error;
                                if (err != null) {
                                    error = null;
                                    child.onError(err);
                                } else {
                                    child.onComplete();
                                }
                                return;
                            } else {
                                // nothing to emit and not done
                                break;
                            }
                        } else {
                            child.onNext(c);
                            e++;
                        }
                    }
                    if (e != 0L && r != Long.MAX_VALUE) {
                        requested.addAndGet(-e);
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }

        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                parent.request(n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            parent.cancel();
        }

    }
}
