package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Maybe;
import io.reactivex.MaybeObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableReduce<T> extends Maybe<T> {

    private final Flowable<T> source;
    private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private final int maxChained;

    public FlowableReduce(Flowable<T> source,
            Function<? super Flowable<T>, ? extends Flowable<T>> reducer, int maxChained) {
        Preconditions.checkArgument(maxChained > 0, "maxChained must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.maxChained = maxChained;
    }

    @Override
    protected void subscribeActual(MaybeObserver<? super T> observer) {

        Flowable<T> f;
        try {
            f = reducer.apply(source);
        } catch (Exception e) {
            Exceptions.throwIfFatal(e);
            observer.onSubscribe(Disposables.empty());
            observer.onError(e);
            return;
        }
        AtomicReference<CountAndFinalSub<T>> info = new AtomicReference<CountAndFinalSub<T>>();
        ReduceDisposable<T> disposable = new ReduceDisposable<T>(info);
        ReduceReplaySubject<T> sub = new ReduceReplaySubject<T>(info, disposable, maxChained,
                reducer, observer);
        info.set(new CountAndFinalSub<T>(1, sub));
        observer.onSubscribe(disposable);
        f.onTerminateDetach() //
                .subscribe(sub);
    }

    /**
     * Requests minimally of upstream and buffers until this subscriber itself
     * is subscribed to. A maximum of {@code maxDepthConcurrent} subscribers can
     * be chained together at any one time.
     * 
     * @param <T>
     *            generic type
     */
    private static final class ReduceReplaySubject<T> extends Flowable<T>
            implements FlowableSubscriber<T>, Subscription {

        // assigned in constructor
        private final AtomicReference<CountAndFinalSub<T>> info;
        private final int maxChained;
        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final MaybeObserver<? super T> observer;
        private final ReduceDisposable<T> disposable;

        // assigned here
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicLong unreconciledRequests = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscriber<? super T>> child = new AtomicReference<Subscriber<? super T>>();
        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();

        // mutable
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private volatile int count;
        private T last;
        private boolean childExists;

        ReduceReplaySubject(AtomicReference<CountAndFinalSub<T>> info,
                ReduceDisposable<T> disposable, int maxDepthConcurrent,
                Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
                MaybeObserver<? super T> observer) {
            this.info = info;
            this.disposable = disposable;
            this.maxChained = maxDepthConcurrent;
            this.reducer = reducer;
            this.observer = observer;
        }

        public boolean cancelled() {
            return cancelled;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.setOnce(this.parent, parent)) {
                unreconciledRequests.incrementAndGet();
                parent.request(1);
            }
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            // only one subscriber expected
            this.child.set(child);
            child.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                Subscription par = parent.get();
                if (par != null) {
                    if (n < Long.MAX_VALUE) {
                        while (true) {
                            long p = unreconciledRequests.get();
                            long r = Math.max(0, n - p);
                            long p2 = p - (n - r);
                            if (unreconciledRequests.compareAndSet(p, p2)) {
                                if (r > 0) {
                                    par.request(r);
                                }
                                break;
                            }
                        }
                    } else {
                        par.request(Long.MAX_VALUE);
                    }
                }
                drain();
            }
        }

        @Override
        public void onNext(T t) {
            if (done) {
                return;
            }
            count++;
            last = t;
            queue.offer(t);
            if (count >= 2) {
                tryToAddSubscriberToChain();
            }
            if (childExists()) {
                drain();
            } else {
                // make minimal request to keep upstream producing
                unreconciledRequests.incrementAndGet();
                Subscription par = parent.get();
                if (par != null) {
                    par.request(1);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            error = t;
            done = true;
            if (childExists()) {
                drain();
            } else {
                cancel();
                cancelWholeChain();
                observer.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            cancelParentAndClear();
            if (count <= 1) {
                // we are finished so report to the observer
                cancelWholeChain();
                if (last == null) {
                    observer.onComplete();
                } else {
                    T t = last;
                    last = null;
                    observer.onSuccess(t);
                }
            } else {
                removeSubscriberFromChain();
                tryToAddSubscriberToChain();
                done = true;
                if (childExists()) {
                    drain();
                }
            }
        }

        private void cancelWholeChain() {
            disposable.dispose();
        }

        private boolean childExists() {
            // do a little dance to avoid volatile reads of child
            if (childExists) {
                return true;
            } else {
                if (child.get() != null) {
                    childExists = true;
                    return true;
                } else {
                    return false;
                }
            }
        }

        private void tryToAddSubscriberToChain() {
            // CAS loop to add another subscriber to the chain if we are not at
            // maxChained and if the final subscriber in the chain has emitted
            // at least 2 elements
            while (true) {
                CountAndFinalSub<T> c = info.get();
                CountAndFinalSub<T> c2;
                if (c.count == maxChained) {
                    c2 = CountAndFinalSub.create(c.count, c.finalSubscriber);
                    if (info.compareAndSet(c, c2)) {
                        // don't subscribe again right now
                        break;
                    }
                } else {
                    ReduceReplaySubject<T> sub = new ReduceReplaySubject<T>(info, disposable,
                            maxChained, reducer, observer);
                    ReduceReplaySubject<T> previous = c.finalSubscriber;
                    if (previous.count >= 2) {
                        // only add a subscriber to the chain once the number of
                        // items received by the final subscriber reaches two
                        c2 = CountAndFinalSub.create(c.count + 1, sub);
                        if (info.compareAndSet(c, c2)) {
                            Flowable<T> f;
                            try {
                                f = reducer.apply(previous);
                            } catch (Exception e) {
                                Exceptions.throwIfFatal(e);
                                cancel();
                                cancelWholeChain();
                                observer.onError(e);
                                return;
                            }
                            f.onTerminateDetach().subscribe(sub);
                        }
                    } else {
                        c2 = CountAndFinalSub.create(c.count, c.finalSubscriber);
                        if (info.compareAndSet(c, c2)) {
                            // don't subscribe again right now
                            break;
                        }
                    }
                }
            }
        }

        private void removeSubscriberFromChain() {
            // CAS loop to reduce the number of chained subscribers by 1
            while (true) {
                CountAndFinalSub<T> c = info.get();
                CountAndFinalSub<T> c2 = CountAndFinalSub.create(c.count - 1, c.finalSubscriber);
                if (info.compareAndSet(c, c2)) {
                    break;
                }
            }
        }

        private void drain() {
            // this is a pretty standard drain loop
            // default is to shortcut errors (don't delay them)
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        boolean d = done;
                        Throwable err = error;
                        if (err != null) {
                            queue.clear();
                            error = null;
                            cancel();
                            child.get().onError(err);
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                cancel();
                                child.get().onComplete();
                                return;
                            } else {
                                break;
                            }
                        } else {
                            child.get().onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        r = requested.addAndGet(-e);
                    }
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelParentAndClear();
            }
        }

        private void cancelParentAndClear() {
            Subscription par = parent.get();
            if (par != null) {
                par.cancel();
                // set parent to null so can be GC'd (to avoid
                // GC nepotism use Flowable.onTerminateDetach()
                // upstream)
                parent.set(null);
            }
        }

    }

    private static final class ReduceDisposable<T> implements Disposable {

        private final AtomicReference<CountAndFinalSub<T>> info;

        ReduceDisposable(AtomicReference<CountAndFinalSub<T>> info) {
            this.info = info;
        }

        @Override
        public void dispose() {
            // CAS loop to dispose final subscriber
            while (true) {
                CountAndFinalSub<T> c = info.get();
                CountAndFinalSub<T> c2 = CountAndFinalSub.create(c.count, c.finalSubscriber);
                if (info.compareAndSet(c, c2)) {
                    c.finalSubscriber.cancel();
                    break;
                }
            }
        }

        @Override
        public boolean isDisposed() {
            return info.get().finalSubscriber.cancelled();
        }
    }

    private static final class CountAndFinalSub<T> {
        final int count;
        final ReduceReplaySubject<T> finalSubscriber;

        static <T> CountAndFinalSub<T> create(int count, ReduceReplaySubject<T> finalSubscriber) {
            return new CountAndFinalSub<T>(count, finalSubscriber);
        }

        CountAndFinalSub(int count, ReduceReplaySubject<T> finalSubscriber) {
            this.count = count;
            this.finalSubscriber = finalSubscriber;
        }

    }
}
