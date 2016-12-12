package com.github.davidmoten.rx2;

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
            }};
    }

}
