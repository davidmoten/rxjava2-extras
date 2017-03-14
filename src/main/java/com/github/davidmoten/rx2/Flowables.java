package com.github.davidmoten.rx2;

import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.rx2.flowable.CachedFlowable;
import com.github.davidmoten.rx2.flowable.CloseableFlowableWithReset;
import com.github.davidmoten.rx2.internal.flowable.*;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class Flowables {

    private static final int DEFAULT_MATCH_BATCH_SIZE = 128;

    private Flowables() {
        // prevent instantiation
    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b, Function<? super A, K> aKey,
            Function<? super B, K> bKey, BiFunction<? super A, ? super B, C> combiner, int requestSize) {
        return new FlowableMatch<A, B, K, C>(a, b, aKey, bKey, combiner, requestSize);
    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b, Function<? super A, K> aKey,
            Function<? super B, K> bKey, BiFunction<? super A, ? super B, C> combiner) {
        return match(a, b, aKey, bKey, combiner, DEFAULT_MATCH_BATCH_SIZE);
    }

    public static <T> Flowable<T> repeat(T t) {
        return new FlowableRepeat<T>(t, -1);
    }

    public static <T> Flowable<T> repeat(T t, long count) {
        return new FlowableRepeat<T>(t, count);
    }

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch,
            long start, int maxConcurrent) {
        return FlowableFetchPagesByRequest.create(fetch, start, maxConcurrent);
    }

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch,
            long start) {
        return fetchPagesByRequest(fetch, start, 2);
    }

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch) {
        return fetchPagesByRequest(fetch, 0, 2);
    }



    /**
     * Returns a cached {@link Flowable} like {@link Flowable#cache()}
     * except that the cache can be reset by calling
     * {@link CachedFlowable#reset()}.
     *
     * @param source
     *            the observable to be cached.
     * @param <T>
     *            the generic type of the source
     * @return a cached observable whose cache can be reset.
     */
    public static <T> CachedFlowable<T> cache(Flowable<T> source) {
        return new CachedFlowable<T>(source);
    }

    /**
     * Returns a cached {@link Flowable} like {@link Flowable#cache()}
     * except that the cache can be reset by calling
     * {@link CachedFlowable#reset()} and the cache will be automatically
     * reset an interval after first subscription (or first subscription after
     * reset). The interval is defined by {@code duration} and {@code unit} .
     *
     * @param source
     *            the source observable
     * @param duration
     *            duration till next reset
     * @param unit
     *            units corresponding to the duration
     * @param worker
     *            worker to use for scheduling reset. Don't forget to
     *            unsubscribe the worker when no longer required.
     * @param <T>
     *            the generic type of the source
     * @return cached observable that resets regularly on a time interval
     */
    public static <T> Flowable<T> cache(final Flowable<T> source, final long duration,
                                        final TimeUnit unit, final Scheduler.Worker worker) {
        final AtomicReference<CachedFlowable<T>> cacheRef = new AtomicReference<CachedFlowable<T>>();
        CachedFlowable<T> cache = new CachedFlowable<T>(source);
        cacheRef.set(cache);
        return cache.doOnSubscribe(new Consumer<Subscription>() {
            @Override
            public void accept(Subscription d) {
                Runnable action = new Runnable() {
                    @Override
                    public void run() {
                        cacheRef.get().reset();
                    }
                };
                worker.schedule(action, duration, unit);
            }
        });
    }

    /**
     * Returns a cached {@link Flowable} like {@link Flowable#cache()}
     * except that the cache may be reset by the user calling
     * {@link CloseableFlowableWithReset#reset}.
     *
     * @param source
     *            the source observable
     * @param duration
     *            duration till next reset
     * @param unit
     *            units corresponding to the duration
     * @param scheduler
     *            scheduler to use for scheduling reset.
     * @param <T>
     *            generic type of source observable
     * @return {@link CloseableFlowableWithReset} that should be closed once
     *         finished to prevent worker memory leak.
     */
    public static <T> CloseableFlowableWithReset<T> cache(final Flowable<T> source,
                                                          final long duration, final TimeUnit unit, final Scheduler scheduler) {
        final AtomicReference<CachedFlowable<T>> cacheRef = new AtomicReference<CachedFlowable<T>>();
        final AtomicReference<Optional<Scheduler.Worker>> workerRef = new AtomicReference<Optional<Scheduler.Worker>>(
                Optional.<Scheduler.Worker> absent());
        CachedFlowable<T> cache = new CachedFlowable<T>(source);
        cacheRef.set(cache);
        Runnable closeAction = new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Optional<Scheduler.Worker> w = workerRef.get();
                    if (w == null) {
                        // we are finished
                        break;
                    } else {
                        if (workerRef.compareAndSet(w, null)) {
                            if (w.isPresent()) {
                                w.get().dispose();
                            }
                            // we are finished
                            workerRef.set(null);
                            break;
                        }
                    }
                    // if not finished then try again
                }
            }
        };
        Runnable resetAction = new Runnable() {

            @Override
            public void run() {
                startScheduledResetAgain(duration, unit, scheduler, cacheRef, workerRef);
            }
        };
        return new CloseableFlowableWithReset<T>(cache, closeAction, resetAction);
    }


    private static <T> void startScheduledResetAgain(final long duration, final TimeUnit unit,
                                                     final Scheduler scheduler, final AtomicReference<CachedFlowable<T>> cacheRef,
                                                     final AtomicReference<Optional<Scheduler.Worker>> workerRef) {

        Runnable action = new Runnable() {
            @Override
            public void run() {
                cacheRef.get().reset();
            }
        };
        // CAS loop to cancel the current worker and create a new one
        while (true) {
            Optional<Scheduler.Worker> wOld = workerRef.get();
            if (wOld == null) {
                // we are finished
                return;
            }
            Optional<Scheduler.Worker> w = Optional.of(scheduler.createWorker());
            if (workerRef.compareAndSet(wOld, w)) {
                if (wOld.isPresent())
                    wOld.get().dispose();
                w.get().schedule(action, duration, unit);
                break;
            }
        }
    }
}
