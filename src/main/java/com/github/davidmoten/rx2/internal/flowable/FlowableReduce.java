package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;
import rx.subscriptions.CompositeSubscription;

public final class FlowableReduce<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private final int maxChained;
    private final long maxIterations;

    public FlowableReduce(Flowable<T> source, Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
            int maxChained, int maxIterations) {
        Preconditions.checkArgument(maxChained > 0, "maxChained must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.maxChained = maxChained;
        this.maxIterations = maxIterations;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {

        Flowable<T> f;
        try {
            f = reducer.apply(source);
        } catch (Exception e) {
            Exceptions.throwIfFatal(e);
            child.onSubscribe(SubscriptionHelper.CANCELLED);
            child.onError(e);
            return;
        }
        AtomicReference<CountAndFinalSub<T>> info = new AtomicReference<CountAndFinalSub<T>>();
        ChainSubscription<T> disposable = new ChainSubscription<T>(info);
        Buffering<T> destination = new Buffering<T>(child, disposable);
        ReduceReplaySubject<T> sub = new ReduceReplaySubject<T>(info, disposable, maxChained, reducer, maxIterations, 1,
                destination);
        info.set(new CountAndFinalSub<T>(1, sub));
        f.onTerminateDetach() //
                .subscribe(sub);
    }

    @SuppressWarnings("serial")
    private static class Buffering<T> extends AtomicInteger implements Subscriber<T>, Subscription {

        private final Subscriber<? super T> child;
        private final ChainSubscription<T> disposable;

        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Throwable error;
        private volatile boolean done;
        private volatile boolean cancelled;

        public Buffering(Subscriber<? super T> child, ChainSubscription<T> chainSubscription) {
            this.child = child;
            this.disposable = chainSubscription;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.deferredSetOnce(this.parent, requested, parent)) {
                child.onSubscribe(new MultiSubscription(this, disposable));
                drain();
            }
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.deferredRequest(this.parent, requested, n);
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this.parent);
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable e) {
            error = e;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        private void drain() {
            // this is a pretty standard drain loop
            // default is to shortcut errors (don't delay them)
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
                        boolean d = done;
                        Throwable err = error;
                        if (err != null) {
                            queue.clear();
                            error = null;
                            cancel();
                            child.onError(err);
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                cancel();
                                child.onComplete();
                                return;
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        r = requested.addAndGet(-e);
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

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
        private final Buffering<T> destination;
        private final ChainSubscription<T> disposable;
        private final long maxIterations;
        private final long iteration;

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

        ReduceReplaySubject(AtomicReference<CountAndFinalSub<T>> info, ChainSubscription<T> disposable,
                int maxDepthConcurrent, Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
                long maxIterations, long iteration, Buffering<T> destination) {
            this.info = info;
            this.disposable = disposable;
            this.maxChained = maxDepthConcurrent;
            this.reducer = reducer;
            this.maxIterations = maxIterations;
            this.iteration = iteration;
            this.destination = destination;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.setOnce(this.parent, parent)) {
                unreconciledRequests.getAndIncrement();
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
        public void onComplete() {
            if (done) {
                return;
            }
            cancelParent();
            if (count <= 1) {
                // we are finished so report to the observer
                cancelWholeChain();
                reportResultToObserver();
            } else {
                done = true;
                decrementChainSize();
                tryToAddSubscriberToChain();
                if (childExists()) {
                    drain();
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
                cancelWholeChain();
                destination.onError(t);
            }
        }

        private void cancelWholeChain() {
            disposable.cancel();
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

        private void reportResultToObserver() {
            T t = last;
            if (t == null) {
                destination.onComplete();
            } else {
                last = null;
                destination.onNext(t);
                destination.onComplete();
            }
        }

        private void tryToAddSubscriberToChain() {
            // CAS loop to add another subscriber to the chain if we are not at
            // maxChained and if the final subscriber in the chain has emitted
            // at least 2 elements
            while (true) {
                CountAndFinalSub<T> c = info.get();
                if (c.count == maxChained) {
                    CountAndFinalSub<T> c2 = CountAndFinalSub.create(c.count, c.finalSubscriber);
                    if (info.compareAndSet(c, c2)) {
                        // don't subscribe again right now
                        break;
                    }
                } else {
                    if (maxIterations != 0 && iteration >= maxIterations - 1) {
                        CountAndFinalSub<T> c2 = CountAndFinalSub.create(c.count + 1, c.finalSubscriber);
                        ReduceReplaySubject<T> previous = c.finalSubscriber;
                        if (info.compareAndSet(c, c2)) {
                            Flowable<T> f;
                            try {
                                f = reducer.apply(previous);
                            } catch (Exception e) {
                                Exceptions.throwIfFatal(e);
                                cancel();
                                cancelWholeChain();
                                destination.onError(e);
                                return;
                            }
                            f.onTerminateDetach().subscribe(destination);
                        }
                    } else if (maxIterations == 0) {
                        ReduceReplaySubject<T> sub = new ReduceReplaySubject<T>(info, disposable, maxChained, reducer,
                                maxIterations, iteration + 1, destination);
                        ReduceReplaySubject<T> previous = c.finalSubscriber;
                        final CountAndFinalSub<T> c2;
                        if (previous.count >= 2) {
                            // only add a subscriber to the chain once the
                            // number of items received by the final subscriber
                            // reaches two
                            c2 = CountAndFinalSub.create(c.count + 1, sub);
                            if (info.compareAndSet(c, c2)) {
                                Flowable<T> f;
                                try {
                                    f = reducer.apply(previous);
                                } catch (Exception e) {
                                    Exceptions.throwIfFatal(e);
                                    cancel();
                                    cancelWholeChain();
                                    destination.onError(e);
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
        }

        private void decrementChainSize() {
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
                cancelParentTryToAddSubscriberToChain();
            }
        }

        private void cancelParentTryToAddSubscriberToChain() {
            cancelParent();
            decrementChainSize();
            tryToAddSubscriberToChain();
        }

        private void cancelParent() {
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

    private static final class ChainSubscription<T> implements Subscription {

        private final AtomicReference<CountAndFinalSub<T>> info;

        ChainSubscription(AtomicReference<CountAndFinalSub<T>> info) {
            this.info = info;
        }

        @Override
        public void cancel() {
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
        public void request(long n) {
            // do nothing
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

    private static final class MultiSubscription implements Subscription {

        private final Subscription primary;
        private final Subscription secondary;

        MultiSubscription(Subscription primary, Subscription secondary) {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public void request(long n) {
            primary.request(n);
        }

        @Override
        public void cancel() {
            primary.cancel();
            secondary.cancel();
        }

    }

}
