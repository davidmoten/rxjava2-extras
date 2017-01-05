package com.github.davidmoten.rx2;

import io.reactivex.functions.Predicate;

public final class Predicates {

    private Predicates() {
        // prevent instantiation
    }

    public static <T> Predicate<T> alwaysFalse() {
        // TODO make holder
        return new Predicate<T>() {

            @Override
            public boolean test(T t) throws Exception {
                return false;
            }
        };
    }

    public static <T> Predicate<T> alwaysTrue() {
        // TODO make holder
        return new Predicate<T>() {

            @Override
            public boolean test(T t) throws Exception {
                return true;
            }
        };
    }

}
