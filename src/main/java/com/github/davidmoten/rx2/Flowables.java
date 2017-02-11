package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.github.davidmoten.rx2.internal.flowable.FlowableMatch;
import com.github.davidmoten.rx2.internal.flowable.FlowableRepeat;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;
import io.reactivex.functions.LongConsumer;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.subjects.ReplaySubject;
import rx.exceptions.Exceptions;

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

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<Long, Long, ? extends Flowable<T>> fetch,
            final long start, final int maxConcurrency) {
        return Flowable.defer(new Callable<Flowable<T>>() {
            @Override
            public Flowable<T> call() throws Exception {
                // need a ReplaySubject because requests can come through before
                // concatEager has established subscriptions to the subject
                final ReplaySubject<Flowable<T>> subject = ReplaySubject.create();
                final AtomicLong position = new AtomicLong(start);
                LongConsumer request = new LongConsumer() {
                    @Override
                    public void accept(long n) throws Exception {
                        long start = position.getAndAdd(n);
                        if (SubscriptionHelper.validate(n)) {
                            Flowable<T> flowable;
                            try {
                                flowable = fetch.apply(start, n);
                            } catch (Throwable e) {
                                Exceptions.throwIfFatal(e);
                                subject.onError(e);
                                return;
                            }
                            subject.onNext(flowable);
                        }
                    }
                };
                return Flowable //
                        .concatEager(subject.serialize() //
                                .toFlowable(BackpressureStrategy.BUFFER), maxConcurrency, 128) //
                        .doOnRequest(request);
            }
        });
    }

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<Long, Long, ? extends Flowable<T>> fetch,
            long start) {
        return fetchPagesByRequest(fetch, start, 2);
    }

    public static <T> Flowable<T> fetchPagesByRequest(final BiFunction<Long, Long, ? extends Flowable<T>> fetch) {
        return fetchPagesByRequest(fetch, 0, 2);
    }
}
