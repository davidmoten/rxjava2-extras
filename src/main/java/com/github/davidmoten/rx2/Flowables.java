package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.internal.flowable.FlowableMatch;
import com.github.davidmoten.rx2.internal.flowable.FlowableRepeat;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

public final class Flowables {

    private static final int _128 = 128;

	private Flowables() {
        // prevent instantiation
    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b, Function<? super A, K> aKey,
            Function<? super B, K> bKey, BiFunction<? super A, ? super B, C> combiner, int requestSize) {
        return new FlowableMatch<A, B, K, C>(a, b, aKey, bKey, combiner, requestSize);
    }

    public static <A, B, K, C> Flowable<C> match(Flowable<A> a, Flowable<B> b, Function<? super A, K> aKey,
            Function<? super B, K> bKey, BiFunction<? super A, ? super B, C> combiner) {
        return match(a, b, aKey, bKey, combiner, _128);
    }

    public static <T> Flowable<T> repeat(T t) {
        return new FlowableRepeat<T>(t, -1);
    }

    public static <T> Flowable<T> repeat(T t, long count) {
        return new FlowableRepeat<T>(t, count);
    }

}
