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
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.disposables.DisposableHelper;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableInsertMaybe<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Function<? super T, ? extends Maybe<? extends T>> valueToInsert;

    public FlowableInsertMaybe(Flowable<T> source, Function<? super T, ? extends Maybe<? extends T>> valueToInsert) {
        Preconditions.checkNotNull(valueToInsert, "valueToInsert cannot be null");
        this.source = source;
        this.valueToInsert = valueToInsert;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> downstream) {
        source.subscribe(new InsertSubscriber<T>(downstream, valueToInsert));
    }

    static final class InsertSubscriber<T> extends AtomicInteger implements FlowableSubscriber<T>, Subscription {

        private static final long serialVersionUID = -15415234346097063L;

        private final Subscriber<? super T> downstream;
        private final Function<? super T, ? extends Maybe<? extends T>> valueToInsert;
        private final SimplePlainQueue<T> queue;
        private final AtomicLong requested;
        private final AtomicLong inserted;

        // define as type Object so can set with a non-null sentinel object that does
        // not gather a stacktrace. A further refinement would be to use just one
        // AtomicReference for `error` and `done` (leaving it for now)
        private final AtomicReference<Object> error;
        private final AtomicReference<Disposable> valueToInsertObserver;

        private Subscription upstream;
        private volatile boolean done;
        private volatile boolean cancelled;

        // used to prevent emission of events after a terminal event
        // does not need to be volatile
        private boolean finished;

        InsertSubscriber(Subscriber<? super T> downstream,
                Function<? super T, ? extends Maybe<? extends T>> valueToInsert) {
            this.downstream = downstream;
            this.valueToInsert = valueToInsert;
            this.queue = new MpscLinkedQueue<T>();
            this.requested = new AtomicLong();
            this.inserted = new AtomicLong();
            this.error = new AtomicReference<Object>();
            this.valueToInsertObserver = new AtomicReference<Disposable>();
        }

        @Override
        public void onSubscribe(Subscription upstream) {
            if (SubscriptionHelper.validate(this.upstream, upstream)) {
                this.upstream = upstream;
                downstream.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            if (finished) {
                return;
            }
            queue.offer(t);
            Maybe<? extends T> maybe;
            try {
                maybe = valueToInsert.apply(t);
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                // we cancel upstream ourselves because the
                // error did not originate from source
                upstream.cancel();
                onError(e);
                return;
            }
            ValueToInsertObserver<T> o = new ValueToInsertObserver<T>(this);
            if (DisposableHelper.set(valueToInsertObserver, o)) {
                // note that at this point we have to cover o being disposed
                // from another thread so the Observer class needs
                // to handle dispose being called before/during onSubscribe
                maybe.subscribe(o);
            }
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (finished) {
                RxJavaPlugins.onError(e);
                return;
            }
            finished = true;
            if (error.compareAndSet(null, e)) {
                DisposableHelper.dispose(valueToInsertObserver);
                done = true;
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
            DisposableHelper.dispose(valueToInsertObserver);
            done = true;
            drain();
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
                        DisposableHelper.dispose(valueToInsertObserver);
                        queue.clear();
                        return;
                    }
                    // must read `done` before polling queue
                    boolean d = done;
                    T t = queue.poll();
                    if (t == null) {
                        if (d) {
                            Object err = error.get();
                            if (err != null) {
                                // clear the exception so can be gc'd
                                // `this` is not a real error, it just prevents
                                // it getting set again in a race because the other
                                // setters which use CAS assume initial value of null
                                error.set(this);
                                DisposableHelper.dispose(valueToInsertObserver);
                                downstream.onError((Throwable) err);
                            } else {
                                // don't need to dispose valueToInsertObserver because already done in
                                // onComplete
                                downstream.onComplete();
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
                DisposableHelper.dispose(valueToInsertObserver);
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

        void insert(T t) {
            inserted.incrementAndGet();
            queue.offer(t);
            drain();
        }

        void insertError(Throwable e) {
            if (error.compareAndSet(null, e)) {
                upstream.cancel();
                DisposableHelper.dispose(valueToInsertObserver);
                done = true;
                drain();
            } else {
                RxJavaPlugins.onError(e);
            }
        }

    }

    static final class ValueToInsertObserver<T> extends AtomicReference<Disposable>
            implements MaybeObserver<T>, Disposable {

        private static final long serialVersionUID = 41384726414575403L;

        private final InsertSubscriber<T> downstream;

        ValueToInsertObserver(InsertSubscriber<T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Disposable upstream) {
            // an AtomicReference is used to hold the upstream Disposable
            // because this Observer can be disposed before onSubscribe
            // is called (contrary to the normal contract).
            DisposableHelper.setOnce(this, upstream);
        }

        @Override
        public void onSuccess(T t) {
            lazySet(DisposableHelper.DISPOSED);
            downstream.insert(t);
        }

        @Override
        public void onError(Throwable e) {
            lazySet(DisposableHelper.DISPOSED);
            downstream.insertError(e);
        }

        @Override
        public void onComplete() {
            lazySet(DisposableHelper.DISPOSED);
            // don't do anything else because no value to insert was reported
        }

        @Override
        public void dispose() {
            DisposableHelper.dispose(this);
        }

        @Override
        public boolean isDisposed() {
            return get() == DisposableHelper.DISPOSED;
        }

    }
}
