package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;

import com.github.davidmoten.rx2.buffertofile.Options;
import com.github.davidmoten.rx2.internal.flowable.FlowableDoOnEmpty;
import com.github.davidmoten.rx2.internal.flowable.FlowableMapLast;
import com.github.davidmoten.rx2.internal.flowable.FlowableMatch;
import com.github.davidmoten.rx2.internal.flowable.FlowableReverse;
import com.github.davidmoten.rx2.internal.flowable.TransformerStateMachine;
import com.github.davidmoten.rx2.util.Pair;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function;
import io.reactivex.functions.Function3;

public final class FlowableTransformers {

    private FlowableTransformers() {
        // prevent instantiation
    }

    public static <State, In, Out> FlowableTransformer<In, Out> stateMachine(Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            BackpressureStrategy backpressureStrategy, int requestBatchSize) {
        return TransformerStateMachine.create(initialState, transition, completion, backpressureStrategy,
                requestBatchSize);
    }

    public static StateMachine.Builder stateMachine() {
        return StateMachine.builder();
    }

    public static <T> FlowableTransformer<T, T> doOnEmpty(final Action action) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> upstream) {
                return new FlowableDoOnEmpty<T>(upstream, action);
            }
        };
    }

    public static <T> FlowableTransformer<T, T> reverse() {
        // TODO make holder
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> upstream) {
                return FlowableReverse.reverse(upstream);
            }

        };
    }

    public static <T> FlowableTransformer<T, T> mapLast(final Function<? super T, ? extends T> function) {
        return new FlowableTransformer<T, T>() {

            @Override
            public Publisher<T> apply(Flowable<T> upstream) {
                return new FlowableMapLast<T>(upstream, function);
            }

        };

    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b, Function<? super A, K> aKey,
            Function<? super B, K> bKey, BiFunction<? super A, ? super B, C> combiner, int requestSize) {
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
                            public Pair<T, Statistics> apply(Pair<T, Statistics> pair, T t) throws Exception {
                                return Pair.create(t, pair.b().add(function.apply(t)));
                            }
                        }).skip(1);
            }
        };
    }
    
}
