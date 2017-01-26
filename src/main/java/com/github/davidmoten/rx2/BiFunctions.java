package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.functions.BiFunction;

public final class BiFunctions {

    private BiFunctions() {
        // prevent instantiation
    }

    @SuppressWarnings("unchecked")
    public static <A, B, C> BiFunction<A, B, C> throwing() {
        return (BiFunction<A, B, C>) ThrowingHolder.INSTANCE;
    }

    private static final class ThrowingHolder {
        static BiFunction<Object, Object, Object> INSTANCE = new BiFunction<Object, Object, Object>() {

            @Override
            public Object apply(Object t1, Object t2) throws Exception {
                throw new ThrowingException();
            }
        };
    }

    public static <T extends Number> BiFunction<Statistics, T, Statistics> collectStats() {
        return new BiFunction<Statistics, T, Statistics>() {

            @Override
            public Statistics apply(Statistics s, T t) {
                return s.add(t);
            }
        };
    }

    public static <T, R, S> BiFunction<T, R, S> constant(final S value) {
        // TODO make holder
        return new BiFunction<T, R, S>() {

            @Override
            public S apply(T t1, R t2) throws Exception {
                return value;
            }
        };
    }

    public static <T, R, S> BiFunction<T, R, S> toNull() {
        // TODO make holder
        return new BiFunction<T, R, S>() {

            @Override
            public S apply(T t1, R t2) throws Exception {
                return null;
            }
        };

    }

}
