package com.github.davidmoten.rx2;

import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.rx2.flowable.CachedFlowable;
import com.github.davidmoten.rx2.observable.CachedObservable;
import com.github.davidmoten.rx2.observable.CloseableObservableWithReset;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class Observables {


    private Observables() {
        // prevent instantiation
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
    public static <T> CachedObservable<T> cache(Observable<T> source) {
        return new CachedObservable<T>(source);
    }

    /**
     * Returns a cached {@link Observable} like {@link Observable#cache()}
     * except that the cache can be reset by calling
     * {@link CachedObservable#reset()} and the cache will be automatically
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
    public static <T> Observable<T> cache(final Observable<T> source, final long duration,
                                        final TimeUnit unit, final Scheduler.Worker worker) {
        final AtomicReference<CachedObservable<T>> cacheRef = new AtomicReference<CachedObservable<T>>();
        CachedObservable<T> cache = new CachedObservable<T>(source);
        cacheRef.set(cache);
        return cache.doOnSubscribe(new Consumer<Disposable>() {
            @Override
            public void accept(Disposable d) {
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
     * Returns a cached {@link Observable} like {@link Observable#cache()}
     * except that the cache may be reset by the user calling
     * {@link CloseableObservableWithReset#reset}.
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
     * @return {@link CloseableObservableWithReset} that should be closed once
     *         finished to prevent worker memory leak.
     */
    public static <T> CloseableObservableWithReset<T> cache(final Observable<T> source,
                                                            final long duration, final TimeUnit unit, final Scheduler scheduler) {
        final AtomicReference<CachedObservable<T>> cacheRef = new AtomicReference<CachedObservable<T>>();
        final AtomicReference<Optional<Scheduler.Worker>> workerRef = new AtomicReference<Optional<Scheduler.Worker>>(
                Optional.<Scheduler.Worker> absent());
        CachedObservable<T> cache = new CachedObservable<T>(source);
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
        return new CloseableObservableWithReset<T>(cache, closeAction, resetAction);
    }


    private static <T> void startScheduledResetAgain(final long duration, final TimeUnit unit,
                                                     final Scheduler scheduler, final AtomicReference<CachedObservable<T>> cacheRef,
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
