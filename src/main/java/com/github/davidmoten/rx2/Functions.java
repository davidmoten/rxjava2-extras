package com.github.davidmoten.rx2;

import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

public final class Functions {

    private Functions() {
        // prevent instantiation
    }

    public static <T> Function<Object, T> constant(final T value) {
        return new Function<Object, T>() {

            @Override
            public T apply(Object t) throws Exception {
                return value;
            }
        };
    }

    public static <T> Function<T, T> identity() {
        // TODO make holder
        return new Function<T, T>() {

            @Override
            public T apply(T t) throws Exception {
                return t;
            }
        };
    }
    
    public static <T, R> Function<T, R> throwing() {
        //TODO make holder
        return new Function<T, R>() {

            @Override
            public R apply(T t) {
                throw new ThrowingException();
            }
        };
    }

    public static final class ThrowingException extends RuntimeException {
        private static final long serialVersionUID = 930909878278758496L;
    }

    public static <A,B,C> BiFunction<A, B, C> throwing2() {
        return new BiFunction<A,B,C>() {

            @Override
            public C apply(A t1, B t2) throws Exception {
                throw new ThrowingException();
            }};
    }


}
