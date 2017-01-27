package com.github.davidmoten.rx2;

import io.reactivex.Maybe;

public final class Maybes {

    private Maybes() {
        // prevent instantiation
    }

    public static <T> Maybe<T> fromNullable(T t) {
        if (t == null) {
            return Maybe.empty();
        } else {
            return Maybe.just(t);
        }
    }

}
