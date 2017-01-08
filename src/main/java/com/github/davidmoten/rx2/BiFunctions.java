package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.functions.BiFunction;

public final class BiFunctions {

    private BiFunctions() {
        // prevent instantiation
    }

    public static <A, B, C> BiFunction<A, B, C> throwing() {
        return new BiFunction<A, B, C>() {

            @Override
            public C apply(A t1, B t2) throws Exception {
                throw new ThrowingException();
            }
        };
    }
}
