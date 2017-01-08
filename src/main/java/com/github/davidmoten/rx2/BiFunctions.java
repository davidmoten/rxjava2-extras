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
    
    private static enum ThrowingHolder {
        ;
        static BiFunction<Object,Object,Object> INSTANCE = new BiFunction<Object, Object, Object>() {

            @Override
            public Object apply(Object t1, Object t2) throws Exception {
                throw new ThrowingException();
            }
        };
    }
}
