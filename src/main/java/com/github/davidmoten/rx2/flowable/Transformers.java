package com.github.davidmoten.rx2.flowable;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx2.BiFunctions;
import com.github.davidmoten.rx2.Flowables;
import com.github.davidmoten.rx2.StateMachine;
import com.github.davidmoten.rx2.StateMachine2;
import com.github.davidmoten.rx2.Statistics;
import com.github.davidmoten.rx2.buffertofile.Options;
import com.github.davidmoten.rx2.internal.flowable.FlowableCollectWhile;
import com.github.davidmoten.rx2.internal.flowable.FlowableDoOnEmpty;
import com.github.davidmoten.rx2.internal.flowable.FlowableMapLast;
import com.github.davidmoten.rx2.internal.flowable.FlowableMatch;
import com.github.davidmoten.rx2.internal.flowable.FlowableMaxRequest;
import com.github.davidmoten.rx2.internal.flowable.FlowableMinRequest;
import com.github.davidmoten.rx2.internal.flowable.FlowableOutputStreamTransform;
import com.github.davidmoten.rx2.internal.flowable.FlowableRepeatingTransform;
import com.github.davidmoten.rx2.internal.flowable.FlowableReverse;
import com.github.davidmoten.rx2.internal.flowable.FlowableWindowMinMax;
import com.github.davidmoten.rx2.internal.flowable.FlowableWindowMinMax.Metric;
import com.github.davidmoten.rx2.internal.flowable.TransformerStateMachine;
import com.github.davidmoten.rx2.util.Pair;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.Notification;
import io.reactivex.Observable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function;
import io.reactivex.functions.Function3;

public final class Transformers {

    private Transformers() {
        // prevent instantiation
    }

    public static <State, In, Out> FlowableTransformer<In, Out> stateMachine(
            Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            BackpressureStrategy backpressureStrategy, int requestBatchSize) {
        return TransformerStateMachine.create(initialState, transition, completion,
                backpressureStrategy, requestBatchSize);
    }

    public static StateMachine.Builder stateMachine() {
        return StateMachine.builder();
    }

    public static StateMachine2.Builder stateMachine2() {
        return StateMachine2.builder();
    }

    /**
     * Returns a transformer that when a stream is empty runs the given
     * {@link Action}.
     * 
     * <p>
     * <img src=
     * "https://raw.githubusercontent.com/davidmoten/rxjava2-extras/master/src/docs/doOnEmpty.png"
     * alt="image">
     * 
     * @param action
     *            to be called when the stream is determined to be empty.
     * @param <T>
     *            item type
     * 
     * @return a transformer that when a stream is empty runs the given action.
     */
    public static <T> FlowableTransformer<T, T> doOnEmpty(final Action action) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> upstream) {
                return new FlowableDoOnEmpty<T>(upstream, action);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> FlowableTransformer<T, T> reverse() {
        return (FlowableTransformer<T, T>) ReverseHolder.INSTANCE;
    }

    private static final class ReverseHolder {
        static final FlowableTransformer<Object, Object> INSTANCE = new FlowableTransformer<Object, Object>() {

            @Override
            public Publisher<Object> apply(Flowable<Object> upstream) {
                return FlowableReverse.reverse(upstream);
            }

        };

    }

    public static <T> FlowableTransformer<T, T> mapLast(
            final Function<? super T, ? extends T> function) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> upstream) {
                return new FlowableMapLast<T>(upstream, function);
            }

        };

    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b,
            Function<? super A, K> aKey, Function<? super B, K> bKey,
            BiFunction<? super A, ? super B, C> combiner, int requestSize) {
        return new FlowableMatch<A, B, K, C>(a, b, aKey, bKey, combiner, requestSize);
    }

    public static <A, B, C, K> FlowableTransformer<A, C> matchWith(final Flowable<B> b,
            final Function<? super A, K> aKey, final Function<? super B, K> bKey,
            final BiFunction<? super A, ? super B, C> combiner, int requestSize) {
        return new FlowableTransformer<A, C>() {

            @Override
            public Publisher<C> apply(Flowable<A> upstream) {
                return Flowables.match(upstream, b, aKey, bKey, combiner);
            }
        };
    }

    public static <A, B, C, K> FlowableTransformer<A, C> matchWith(final Flowable<B> b,
            final Function<? super A, K> aKey, final Function<? super B, K> bKey,
            final BiFunction<? super A, ? super B, C> combiner) {
        return matchWith(b, aKey, bKey, combiner, 128);
    }

    public static Options.BuilderFlowable onBackpressureBufferToFile() {
        return Options.builderFlowable();
    }

    /**
     * <p>
     * Converts a stream of {@code Number} to a stream of {@link Statistics} about
     * those numbers.
     * 
     * <p>
     * <img src=
     * "https://raw.githubusercontent.com/davidmoten/rxjava2-extras/master/src/docs/collectStats.png"
     * alt="image">
     * 
     * @param <T>
     *            item type
     * @return transformer that converts a stream of Number to a stream of
     *         Statistics
     */
    @SuppressWarnings("unchecked")
    public static <T extends Number> FlowableTransformer<T, Statistics> collectStats() {
        return (FlowableTransformer<T, Statistics>) CollectStatsHolder.INSTANCE;
    }

    private static final class CollectStatsHolder {
        static final FlowableTransformer<Number, Statistics> INSTANCE = new FlowableTransformer<Number, Statistics>() {

            @Override
            public Flowable<Statistics> apply(Flowable<Number> source) {
                return source.scan(Statistics.create(), BiFunctions.collectStats());
            }
        };
    }

    public static <T, R extends Number> FlowableTransformer<T, Pair<T, Statistics>> collectStats(
            final Function<? super T, ? extends R> function) {
        return new FlowableTransformer<T, Pair<T, Statistics>>() {

            @Override
            public Flowable<Pair<T, Statistics>> apply(Flowable<T> source) {
                return source.scan(Pair.create((T) null, Statistics.create()),
                        new BiFunction<Pair<T, Statistics>, T, Pair<T, Statistics>>() {
                            @Override
                            public Pair<T, Statistics> apply(Pair<T, Statistics> pair, T t)
                                    throws Exception {
                                return Pair.create(t, pair.b().add(function.apply(t)));
                            }
                        }).skip(1);
            }
        };
    }

    /**
     * Returns a transformer that emits collections of items with the collection
     * boundaries determined by the given {@link BiPredicate}.
     * 
     * <p>
     * <img src=
     * "https://raw.githubusercontent.com/davidmoten/rxjava2-extras/master/src/docs/collectWhile.png"
     * alt="image">
     * 
     * @param collectionFactory
     *            factory to create a new collection
     * @param add
     *            method to add an item to a collection
     * @param condition
     *            while true will continue to add to the current collection
     * @param emitRemainder
     *            whether to emit the remainder as a collection
     * @param <T>
     *            item type
     * @param <R>
     *            collection type
     * @return transform that collects while some conditions is returned then starts
     *         a new collection
     */
    public static <T, R> FlowableTransformer<T, R> collectWhile(final Callable<R> collectionFactory,
            final BiFunction<? super R, ? super T, ? extends R> add,
            final BiPredicate<? super R, ? super T> condition, final boolean emitRemainder) {
        return new FlowableTransformer<T, R>() {

            @Override
            public Publisher<R> apply(Flowable<T> source) {
                return new FlowableCollectWhile<T, R>(source, collectionFactory, add, condition,
                        emitRemainder);
            }
        };
    }

    public static <T, R> FlowableTransformer<T, R> collectWhile(final Callable<R> collectionFactory,
            final BiFunction<? super R, ? super T, ? extends R> add,
            final BiPredicate<? super R, ? super T> condition) {
        return collectWhile(collectionFactory, add, condition, true);
    }

    public static <T> FlowableTransformer<T, List<T>> toListWhile(
            final BiPredicate<? super List<T>, ? super T> condition, boolean emitRemainder) {
        return collectWhile(ListFactoryHolder.<T>factory(), ListFactoryHolder.<T>add(), condition,
                emitRemainder);
    }

    public static <T> FlowableTransformer<T, List<T>> toListWhile(
            final BiPredicate<? super List<T>, ? super T> condition) {
        return toListWhile(condition, true);
    }

    public static <T> FlowableTransformer<T, List<T>> bufferWhile(
            final BiPredicate<? super List<T>, ? super T> condition, boolean emitRemainder) {
        return toListWhile(condition, emitRemainder);
    }

    public static <T> FlowableTransformer<T, List<T>> bufferWhile(
            final BiPredicate<? super List<T>, ? super T> condition) {
        return toListWhile(condition);
    }

    private static final class ListFactoryHolder {

        private static final Callable<List<Object>> INSTANCE = new Callable<List<Object>>() {

            @Override
            public List<Object> call() throws Exception {
                return new ArrayList<Object>();
            }
        };

        private static final BiFunction<List<Object>, Object, List<Object>> ADD = new BiFunction<List<Object>, Object, List<Object>>() {

            @Override
            public List<Object> apply(List<Object> list, Object t) throws Exception {
                list.add(t);
                return list;
            }
        };

        @SuppressWarnings("unchecked")
        static <T> Callable<List<T>> factory() {
            return (Callable<List<T>>) (Callable<?>) INSTANCE;
        };

        @SuppressWarnings("unchecked")
        static <T> BiFunction<List<T>, T, List<T>> add() {
            return (BiFunction<List<T>, T, List<T>>) (BiFunction<?, ?, ?>) ADD;
        }

    }

    public static <T extends Comparable<T>> FlowableTransformer<T, T> windowMax(
            final int windowSize) {
        return windowMax(windowSize, Transformers.<T>naturalComparator());
    }

    public static <T> FlowableTransformer<T, T> windowMax(final int windowSize,
            final Comparator<? super T> comparator) {
        return new FlowableTransformer<T, T>() {
            @Override
            public Flowable<T> apply(Flowable<T> source) {
                return new FlowableWindowMinMax<T>(source, windowSize, comparator, Metric.MAX);
            }
        };
    }

    public static <T extends Comparable<T>> FlowableTransformer<T, T> windowMin(
            final int windowSize) {
        return windowMin(windowSize, Transformers.<T>naturalComparator());
    }

    public static <T> FlowableTransformer<T, T> windowMin(final int windowSize,
            final Comparator<? super T> comparator) {
        return new FlowableTransformer<T, T>() {
            @Override
            public Flowable<T> apply(Flowable<T> source) {
                return new FlowableWindowMinMax<T>(source, windowSize, comparator, Metric.MIN);
            }
        };
    }

    private static class NaturalComparatorHolder {
        static final Comparator<Comparable<Object>> INSTANCE = new Comparator<Comparable<Object>>() {

            @Override
            public int compare(Comparable<Object> o1, Comparable<Object> o2) {
                return o1.compareTo(o2);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> Comparator<T> naturalComparator() {
        return (Comparator<T>) (Comparator<?>) NaturalComparatorHolder.INSTANCE;
    }

    public static <T> FlowableTransformer<T, T> maxRequest(final long... maxRequest) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> source) {
                return new FlowableMaxRequest<T>(source, maxRequest);
            }
        };
    }

    public static <T> FlowableTransformer<T, T> minRequest(final int... minRequests) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> source) {
                return new FlowableMinRequest<T>(source, minRequests);
            }
        };
    }

    public static <T> FlowableTransformer<T, T> rebatchRequests(final int minRequest,
            final long maxRequest, final boolean constrainFirstRequestMin) {
        Preconditions.checkArgument(minRequest <= maxRequest,
                "minRequest cannot be greater than maxRequest");
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> source) {
                if (minRequest == maxRequest && constrainFirstRequestMin) {
                    return source.rebatchRequests(minRequest);
                } else {
                    return source
                            .compose(Transformers.<T>minRequest(
                                    constrainFirstRequestMin ? minRequest : 1, minRequest))
                            .compose(Transformers.<T>maxRequest(maxRequest));
                }
            }
        };
    }

    public static <T> FlowableTransformer<T, T> rebatchRequests(int minRequest, long maxRequest) {
        return rebatchRequests(minRequest, maxRequest, true);
    }

    public static <T> Function<Flowable<T>, Flowable<T>> repeat(
            final Function<? super Flowable<T>, ? extends Flowable<T>> transform,
            final int maxChained, final long maxIterations,
            final Function<Observable<T>, Observable<?>> tester) {
        Preconditions.checkArgument(maxChained > 0, "maxChained must be > 0");
        Preconditions.checkArgument(maxIterations > 0, "maxIterations must be > 0");
        Preconditions.checkNotNull(transform, "transform must not be null");
        Preconditions.checkNotNull(tester, "tester must not be null");
        return new Function<Flowable<T>, Flowable<T>>() {
            @Override
            public Flowable<T> apply(Flowable<T> source) {
                return new FlowableRepeatingTransform<T>(source, transform, maxChained,
                        maxIterations, tester);
            }
        };
    }

    public static <T> Function<Flowable<T>, Flowable<T>> reduce(
            final Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
            final int maxChained, final long maxIterations) {
        return repeat(reducer, maxChained, maxIterations, Transformers.<T>finishWhenSingle());
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<Observable<T>, Observable<?>> finishWhenSingle() {
        return (Function<Observable<T>, Observable<?>>) (Function<?, Observable<?>>) FINISH_WHEN_SINGLE;
    }

    private static final Function<Observable<Object>, Observable<?>> FINISH_WHEN_SINGLE = new Function<Observable<Object>, Observable<?>>() {

        @Override
        public Observable<?> apply(final Observable<Object> o) throws Exception {
            return Observable.defer(new Callable<Observable<Object>>() {

                final long[] count = new long[1];

                @Override
                public Observable<Object> call() throws Exception {
                    return o.materialize() //
                            .flatMap(
                                    new Function<Notification<Object>, Observable<Notification<Object>>>() {
                                        @Override
                                        public Observable<Notification<Object>> apply(
                                                Notification<Object> x) throws Exception {
                                            if (x.isOnNext()) {
                                                count[0]++;
                                                if (count[0] > 1) {
                                                    return Observable.just(x);
                                                } else {
                                                    return Observable.empty();
                                                }
                                            } else if (x.isOnComplete()) {
                                                if (count[0] <= 1) {
                                                    // complete the stream
                                                    return Observable.just(x);
                                                } else {
                                                    // never complete
                                                    return Observable.never();
                                                }
                                            } else {
                                                // is onError
                                                return Observable.just(x);
                                            }
                                        }
                                    }) //
                            .dematerialize();
                }
            });
        }
    };

    public static <T> Function<Flowable<T>, Flowable<T>> reduce(
            final Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
            final int maxChained) {
        return reduce(reducer, maxChained, Long.MAX_VALUE);
    }

    public static FlowableTransformer<byte[], byte[]> outputStream(
            final Function<OutputStream, OutputStream> transform, final int bufferSize,
            final int batchSize) {
        return new FlowableTransformer<byte[], byte[]>() {

            @Override
            public Publisher<byte[]> apply(final Flowable<byte[]> f) {
                return new FlowableOutputStreamTransform(f, transform, bufferSize, batchSize);
            }

        };
    }

    public static FlowableTransformer<byte[], byte[]> outputStream(
            final Function<OutputStream, OutputStream> transform) {
        return outputStream(transform, 8192, 16);
    }
}
