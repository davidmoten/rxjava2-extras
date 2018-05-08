package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.rx2.flowable.CachedFlowable;
import com.github.davidmoten.rx2.flowable.CloseableFlowableWithReset;
import com.github.davidmoten.rx2.internal.flowable.FlowableDeepTransform;
import com.github.davidmoten.rx2.internal.flowable.FlowableFetchPagesByRequest;
import com.github.davidmoten.rx2.internal.flowable.FlowableMatch;
import com.github.davidmoten.rx2.internal.flowable.FlowableMergeInterleaved;
import com.github.davidmoten.rx2.internal.flowable.FlowableRepeat;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

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

    /**
     * <p>
     * Creates a Flowable that is aimed at supporting calls to a service that
     * provides data in pages where the page sizes are determined by requests from
     * downstream (requests are a part of the backpressure machinery of RxJava).
     * 
     * <p>
     * <img src=
     * "https://raw.githubusercontent.com/davidmoten/rxjava2-extras/master/src/docs/fetchPagesByRequest.png"
     * alt="image">
     * 
     * <p>
     * Here's an example.
     * 
     * <p>
     * Suppose you have a stateless web service, say a rest service that returns
     * JSON/XML and supplies you with
     * 
     * <ul>
     * <li>the most popular movies of the last 24 hours sorted by descending
     * popularity</li>
     * </ul>
     * 
     * <p>
     * The service supports paging in that you can pass it a start number and a page
     * size and it will return just that slice from the list.
     * 
     * <p>
     * Now I want to give a library with a Flowable definition of this service to my
     * colleagues that they can call in their applications whatever they may be. For
     * example,
     * 
     * <ul>
     * <li>Fred may just want to know the most popular movie each day,</li>
     * <li>Greta wants to get the top 20 and then have the ability to keep scrolling
     * down the list in her UI.</li>
     * </ul>
     * 
     * <p>
     * Let's see how we can efficiently support those use cases. I'm going to assume
     * that the movie data returned by the service are mapped conveniently to
     * objects by whatever framework I'm using (JAXB, Jersey, etc.). The fetch
     * method looks like this:
     * 
     * <pre>
     * {@code
     * // note that start is 0-based
     * List<Movie> mostPopularMovies(int start, int size);
     * }
     * </pre>
     * 
     * <p>
     * Now I'm going to wrap this synchronous call as a Flowable to give to my
     * colleagues:
     * 
     * <pre>
     * {@code
     * Flowable<Movie> mostPopularMovies(int start) {
     *     return Flowables.fetchPagesByRequest(
     *           (position, n) -> Flowable.fromIterable(mostPopular(position, n)),
     *           start)
     *         // rebatch requests so that they are always between 
     *         // 5 and 100 except for the first request
     *       .compose(Transformers.rebatchRequests(5, 100, false));
     * }
     * 
     * Flowable<Movie> mostPopularMovies() {
     *     return mostPopularMovies(0);
     * }
     * }
     * </pre>
     * <p>
     * Note particularly that the method above uses a variant of rebatchRequests to
     * limit both minimum and maximum requests. We particularly don't want to allow
     * a single call requesting the top 100,000 popular movies because of the memory
     * and network pressures that arise from that call.
     * 
     * <p>
     * Righto, Fred now uses the new API like this:
     * 
     * <pre>
     * {
     *     &#64;code
     *     Movie top = mostPopularMovies().compose(Transformers.maxRequest(1)).first().blockingFirst();
     * }
     * </pre>
     * <p>
     * The use of maxRequest above may seem unnecessary but strangely enough the
     * first operator requests Long.MAX_VALUE of upstream and cancels as soon as one
     * arrives. The take, elemnentAt and firstXXX operators all have this
     * counter-intuitive characteristic.
     * 
     * <p>
     * Greta uses the new API like this:
     * 
     * <pre>
     * {@code
     * mostPopularMovies()
     *     .rebatchRequests(20)
     *     .doOnNext(movie -> addToUI(movie))
     *     .subscribe(subscriber);
     * }
     * </pre>
     * <p>
     * A bit more detail about fetchPagesByRequest:
     * 
     * <p>
     * If the fetch function returns a Flowable that delivers fewer than the
     * requested number of items then the overall stream completes.
     * 
     * @param fetch
     *            a function that takes a position index and a length and returns a
     *            Flowable
     * @param start
     *            the start index
     * @param maxConcurrent
     *            how many pages to request concurrently
     * @param <T>
     *            item type
     * @return Flowable that fetches pages based on request amounts
     */
    public static <T> Flowable<T> fetchPagesByRequest(
            final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch, long start, int maxConcurrent) {
        return FlowableFetchPagesByRequest.create(fetch, start, maxConcurrent);
    }

    public static <T> Flowable<T> fetchPagesByRequest(
            final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch, long start) {
        return fetchPagesByRequest(fetch, start, 2);
    }

    public static <T> Flowable<T> fetchPagesByRequest(
            final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch) {
        return fetchPagesByRequest(fetch, 0, 2);
    }

    /**
     * Returns a cached {@link Flowable} like {@link Flowable#cache()} except that
     * the cache can be reset by calling {@link CachedFlowable#reset()}.
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
     * Returns a cached {@link Flowable} like {@link Flowable#cache()} except that
     * the cache can be reset by calling {@link CachedFlowable#reset()} and the
     * cache will be automatically reset an interval after first subscription (or
     * first subscription after reset). The interval is defined by {@code duration}
     * and {@code unit} .
     *
     * @param source
     *            the source observable
     * @param duration
     *            duration till next reset
     * @param unit
     *            units corresponding to the duration
     * @param worker
     *            worker to use for scheduling reset. Don't forget to unsubscribe
     *            the worker when no longer required.
     * @param <T>
     *            the generic type of the source
     * @return cached observable that resets regularly on a time interval
     */
    public static <T> Flowable<T> cache(final Flowable<T> source, final long duration, final TimeUnit unit,
            final Scheduler.Worker worker) {
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
     * Returns a cached {@link Flowable} like {@link Flowable#cache()} except that
     * the cache may be reset by the user calling
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
    public static <T> CloseableFlowableWithReset<T> cache(final Flowable<T> source, final long duration,
            final TimeUnit unit, final Scheduler scheduler) {
        final AtomicReference<CachedFlowable<T>> cacheRef = new AtomicReference<CachedFlowable<T>>();
        final AtomicReference<Optional<Scheduler.Worker>> workerRef = new AtomicReference<Optional<Scheduler.Worker>>(
                Optional.<Scheduler.Worker>absent());
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

    private static final int DEFAULT_RING_BUFFER_SIZE = 128;

    public static <T> Flowable<T> mergeInterleaved(Publisher<? extends Publisher<? extends T>> publishers,
            int maxConcurrency, int batchSize, boolean delayErrors) {
        return new FlowableMergeInterleaved<T>(publishers, maxConcurrency, batchSize, delayErrors);
    }

    public static <T> Flowable<T> mergeInterleaved(Publisher<? extends Publisher<? extends T>> publishers,
            int maxConcurrency) {
        return mergeInterleaved(publishers, maxConcurrency, 128, false);
    }

    public static <T> MergeInterleaveBuilder<T> mergeInterleaved(
            Publisher<? extends Publisher<? extends T>> publishers) {
        return new MergeInterleaveBuilder<T>(publishers);
    }

    public static final class MergeInterleaveBuilder<T> {

        private final Publisher<? extends Publisher<? extends T>> publishers;
        private int maxConcurrency = 4;
        private int batchSize = DEFAULT_RING_BUFFER_SIZE;
        private boolean delayErrors = false;

        MergeInterleaveBuilder(Publisher<? extends Publisher<? extends T>> publishers) {
            this.publishers = publishers;
        }

        public MergeInterleaveBuilder<T> maxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        public MergeInterleaveBuilder<T> batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public MergeInterleaveBuilder<T> delayErrors(boolean delayErrors) {
            this.delayErrors = delayErrors;
            return this;
        }

        public Flowable<T> build() {
            return mergeInterleaved(publishers, maxConcurrency, batchSize, delayErrors);
        }
    }

    public static <T> Flowable<T> deepTransform(final Flowable<T> flowable,
            BiFunction<Flowable<T>, Runnable, Flowable<T>> transform) {
        return new FlowableDeepTransform<T>(flowable, transform);
    }

    public static <T> Flowable<T> deepTransform(final Flowable<T> flowable,
            Function<Flowable<T>, Flowable<T>> transform) {
        return deepTransform(flowable, new BiFunction<Flowable<T>, Runnable, Flowable<T>>() {

            @Override
            public Flowable<T> apply(final Flowable<T> f, final Runnable b) throws Exception {
                return Flowable.defer(new Callable<Publisher<? extends T>>() {
                    @Override
                    public Publisher<? extends T> call() throws Exception {
                        final long[] count = new long[1];
                        return f.doOnNext(new Consumer<T>() {
                            @Override
                            public void accept(T x) throws Exception {
                                count[0]++;
                            }
                        }).doOnComplete(new Action() {
                            @Override
                            public void run() throws Exception {
                                if (count[0] == 1) {
                                    b.run();
                                }
                            }
                        });
                    }
                });

            }
        });
    }
}
