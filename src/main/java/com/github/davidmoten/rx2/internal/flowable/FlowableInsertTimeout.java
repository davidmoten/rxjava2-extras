package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.disposables.DisposableHelper;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableInsertTimeout<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Function<? super T, ? extends Long> timeout;
    private final TimeUnit unit;
    private final Function<? super T, ? extends T> value;
    private final Scheduler scheduler;

    public FlowableInsertTimeout(Flowable<T> source, Function<? super T, ? extends Long> timeout, TimeUnit unit,
            Function<? super T, ? extends T> value, Scheduler scheduler) {
        Preconditions.checkNotNull(timeout, "timeout cannot be null");
        Preconditions.checkNotNull(unit, "unit cannot be null");
        Preconditions.checkNotNull(value, "value cannot be null");
        Preconditions.checkNotNull(scheduler, "scheduler cannot be null");
        this.source = source;
        this.timeout = timeout;
        this.unit = unit;
        this.value = value;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> downstream) {
        source.subscribe(new InsertTimeoutSubscriber<T>(downstream, timeout, unit, value, scheduler));
    }

    static final class InsertTimeoutSubscriber<T> extends AtomicInteger implements FlowableSubscriber<T>, Subscription {

        private static final long serialVersionUID = 1812803226317104954L;

        private static final Object TERMINATED = new Object();

        private final Subscriber<? super T> downstream;
        private final Function<? super T, ? extends Long> timeout;
        private final TimeUnit unit;
        private final Function<? super T, ? extends T> value;

        private final SimplePlainQueue<T> queue;
        private final AtomicLong requested;
        private final AtomicLong inserted;

        // set either with null (not terminated), Throwable (error), or
        // TERMINATED (completion). TERMINATED is also used to replace an 
        // error after it is reported downstream so that the local copy of the 
        // error can be gc'd
        private final AtomicReference<Object> terminated;
        private final AtomicReference<Disposable> scheduled;

        private Subscription upstream;
        private volatile boolean cancelled;

        // used to prevent emission of events after a terminal event
        // does not need to be volatile
        private boolean finished;

        private final Worker worker;

        InsertTimeoutSubscriber(Subscriber<? super T> downstream, Function<? super T, ? extends Long> timeout,
                TimeUnit unit, Function<? super T, ? extends T> value, Scheduler scheduler) {
            this.downstream = downstream;
            this.timeout = timeout;
            this.unit = unit;
            this.value = value;
            this.queue = new MpscLinkedQueue<T>();
            this.requested = new AtomicLong();
            this.inserted = new AtomicLong();
            this.terminated = new AtomicReference<Object>();
            this.scheduled = new AtomicReference<Disposable>();
            this.worker = scheduler.createWorker();
        }

        @Override
        public void onSubscribe(Subscription upstream) {
            if (SubscriptionHelper.validate(this.upstream, upstream)) {
                this.upstream = upstream;
                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onNext(final T t) {
            if (finished) {
                return;
            }
            queue.offer(t);
            final long waitTime;
            try {
                waitTime = timeout.apply(t);
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                // we cancel upstream ourselves because the
                // error did not originate from source
                upstream.cancel();
                onError(e);
                return;
            }
            TimeoutAction<T> action = new TimeoutAction<T>(this, t);
            Disposable d = worker.schedule(action, waitTime, unit);
            DisposableHelper.set(scheduled, d);
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (finished) {
                RxJavaPlugins.onError(e);
                return;
            }
            finished = true;
            if (terminated.compareAndSet(null, e)) {
                dispose();
                drain();
            } else {
                RxJavaPlugins.onError(e);
            }
        }

        @Override
        public void onComplete() {
            if (finished) {
                return;
            }
            finished = true;
            if (terminated.compareAndSet(null, TERMINATED)) {
                dispose();
                drain();
            }
        }

        private void drain() {
            if (getAndIncrement() != 0) {
                return;
            }
            // note that this drain loop does not shortcut errors
            int missed = 1;
            while (true) {
                long r = requested.get();
                long e = 0;
                while (e != r) {
                    if (cancelled) {
                        dispose();
                        queue.clear();
                        return;
                    }
                    Object d = terminated.get();
                    T t = queue.poll();
                    if (t == null) {
                        if (d != null) {
                            // d will be either TERMINATED or a Throwable
                            if (d == TERMINATED) {
                                // don't need to dispose scheduled because already done in
                                // onComplete
                                downstream.onComplete();
                            } else {
                                // clear the exception so can be gc'd
                                // setting the value to TERMINATED just prevents it getting set again in a race
                                // because the other setters which use CAS assume initial value of null
                                terminated.set(TERMINATED);
                                dispose();
                                downstream.onError((Throwable) d);
                            }
                            return;
                        } else {
                            // nothing to emit and not done
                            break;
                        }
                    } else {
                        downstream.onNext(t);
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

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                // modify request to upstream to account for inserted values
                // use a CAS loop because request can be called from any thread
                while (true) {
                    long ins = inserted.get();
                    long d = Math.min(ins, n);
                    if (inserted.compareAndSet(ins, ins - d)) {
                        if (n - d > 0) {
                            upstream.request(n - d);
                        }
                        break;
                    }
                }
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                upstream.cancel();
                dispose();
                if (getAndIncrement() == 0) {
                    // use the same access control to queue as drain method
                    // because `clear` just calls `queue.poll()` repeatedly till nothing left on the
                    // queue (ignoring the dequeued items).
                    //
                    // this is best endeavours, there still exists a race with onNext and drain
                    // where items could be left on the queue after cancel
                    queue.clear();
                }
            }
        }

        private void dispose() {
            DisposableHelper.dispose(scheduled);
            worker.dispose();
        }
        
        void insert(T t) {
            inserted.incrementAndGet();
            queue.offer(t);
            drain();
        }

        void insertError(Throwable e) {
            if (terminated.compareAndSet(null, e)) {
                upstream.cancel();
                dispose();
                drain();
            } else {
                RxJavaPlugins.onError(e);
            }
        }

        T calculateValueToInsert(T t) throws Exception {
            return value.apply(t);
        }

    }

    private static final class TimeoutAction<T> implements Runnable {

        private final InsertTimeoutSubscriber<T> subscriber;
        private final T t;

        TimeoutAction(InsertTimeoutSubscriber<T> subscriber, T t) {
            this.subscriber = subscriber;
            this.t = t;
        }

        @Override
        public void run() {
            final T v;
            try {
                v = subscriber.calculateValueToInsert(t);
            } catch (Throwable e) {
                subscriber.insertError(e);
                return;
            }
            subscriber.insert(v);
        }

    }

}
