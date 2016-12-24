package com.github.davidmoten.rx2;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.functions.Consumer;
import io.reactivex.functions.LongConsumer;

public final class Consumers {

    private Consumers() {
        // prevent instantiation
    }

    public static LongConsumer addLongTo(final List<Long> list) {
        return new LongConsumer() {

            @Override
            public void accept(long t) throws Exception {
                list.add(t);
            }

        };
    }

    public static <T extends Closeable> Consumer<T> close() {
        // TODO create holder
        return new Consumer<T>() {
            @Override
            public void accept(T t) throws Exception {
                t.close();
            }

        };
    }

    public static Consumer<Object> increment(final AtomicInteger value) {
        return new Consumer<Object>() {
            @Override
            public void accept(Object t) throws Exception {
                value.incrementAndGet();
            }
        };
    }

    public static Consumer<Throwable> printStackTrace() {
        // TODO make holder
        return new Consumer<Throwable>() {
            @Override
            public void accept(Throwable e) throws Exception {
                e.printStackTrace();
            }
        };
    }

    public static Consumer<Object> doNothing() {
        // TODO make holder
        return new Consumer<Object>() {

            @Override
            public void accept(Object t) throws Exception {
                // do nothing
            }
        };
    }

    public static <T> Consumer<T> set(final AtomicReference<T> value) {
        return new Consumer<T>() {

            @Override
            public void accept(T t) throws Exception {
                value.set(t);
            }
        };
    }
    
    public static Consumer<Integer> set(final AtomicInteger value) {
        return new Consumer<Integer>() {
            @Override
            public void accept(Integer t) throws Exception {
                value.set(t);
            }
        };
    }

    public static Consumer<Object> decrement(final AtomicInteger value) {
        return new Consumer<Object>() {

            @Override
            public void accept(Object t) throws Exception {
                value.decrementAndGet();
            }

        };
    }

    public static Consumer<Object> setToTrue(final AtomicBoolean value) {
        return new Consumer<Object>() {

            @Override
            public void accept(Object t) throws Exception {
                value.set(true);
            }
        };
    }

    public static <T> Consumer<T> addTo(final List<T> list) {
        return new Consumer<T>() {

            @Override
            public void accept(T t) throws Exception {
                list.add(t);
            }};
    }

    public static <T> Consumer<T> println() {
        // TODO make holder
        return new Consumer<T>() {
            @Override
            public void accept(T t) throws Exception {
                System.out.println(t);
            }};
    }
}
