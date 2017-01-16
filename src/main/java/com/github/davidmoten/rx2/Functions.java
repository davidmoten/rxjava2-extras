package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.exceptions.ThrowingException;

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

    @SuppressWarnings("unchecked")
    public static <T> Function<T, T> identity() {
        return (Function<T, T>) IdentityHolder.INSTANCE;
    }
    
    private static final class IdentityHolder {
        static final Function<Object,Object> INSTANCE = new Function<Object, Object>() {

            @Override
            public Object apply(Object t) throws Exception {
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

}
