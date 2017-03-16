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
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableReduce<T> extends Maybe<T> {

    private final Flowable<T> source;
    private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private final int maxDepthConcurrent;

    public FlowableReduce(Flowable<T> source, Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
            int maxDepthConcurrent) {
        Preconditions.checkArgument(maxDepthConcurrent > 0, "depth must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.maxDepthConcurrent = maxDepthConcurrent;
    }

    @Override
    protected void subscribeActual(MaybeObserver<? super T> observer) {
        AtomicInteger numLevels = new AtomicInteger(1);
        Flowable<T> f;
        try {
            f = reducer.apply(source);
        } catch (Exception e) {
            observer.onError(e);
            return;
        }
        f.onTerminateDetach() //
                .subscribe(new ReduceReplaySubject<T>(numLevels, maxDepthConcurrent, reducer, observer));
    }

    /**
     * Requests minimally of upstream and buffers until this subscriber itself
     * is subscribed to.
     * 
     * @param <T>
     *            generic type
     */
    private static final class ReduceReplaySubject<T> extends Flowable<T>
            implements FlowableSubscriber<T>, Subscription {

        private final AtomicInteger numLevels;
        private final int maxDepthConcurrent;
        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final MaybeObserver<? super T> observer;
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicLong unreconciledRequests = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscriber<? super T>> child = new AtomicReference<Subscriber<? super T>>();

        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private int count;
        private T last;

        ReduceReplaySubject(AtomicInteger numLevels, int maxDepthConcurrent,
                Function<? super Flowable<T>, ? extends Flowable<T>> reducer, MaybeObserver<? super T> observer) {
            this.numLevels = numLevels;
            this.maxDepthConcurrent = maxDepthConcurrent;
            this.reducer = reducer;
            this.observer = observer;
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
            if (count == 2) {
                if (numLevels.get() < maxDepthConcurrent) {
                    numLevels.incrementAndGet();
                    Flowable<T> f;
                    try {
                        f = reducer.apply(this);
                    } catch (Exception e) {
                        onError(e);
                        return;
                    }
                    ReduceReplaySubject<T> sub = new ReduceReplaySubject<T>(numLevels, maxDepthConcurrent, reducer,
                            observer);
                    f.onTerminateDetach().subscribe(sub);
                }
            }
            if (child.get() != null) {
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
            if (child.get() != null) {
                drain();
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            if (count <= 1) {
                // we are finished so report to the observer
                cancel();
                if (last == null) {
                    observer.onComplete();
                } else {
                    T t = last;
                    last = null;
                    observer.onSuccess(t);
                }
            } else {
                numLevels.decrementAndGet();
                done = true;
                if (child.get() != null) {
                    drain();
                }
            }
        }

        private void drain() {
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
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                parent.get().cancel();
                                Throwable err = error;
                                if (err != null) {
                                    error = null;
                                    child.get().onError(err);
                                } else {
                                    child.get().onComplete();
                                }
                                // set parent to null so can be GC'd (to avoid
                                // GC nepotism use Flowable.onTerminateDetach()
                                // upstream)
                                parent.set(null);
                                return;
                            } else {
                                break;
                            }
                        } else {
                            last = t;
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
            cancelled = true;
            Subscription par = parent.get();
            if (par != null) {
                par.cancel();
            }
        }

    }

}
