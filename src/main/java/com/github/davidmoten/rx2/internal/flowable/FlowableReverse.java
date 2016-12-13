package com.github.davidmoten.rx2.internal.flowable;

import java.util.Iterator;
import java.util.List;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

public final class FlowableReverse {

    private FlowableReverse() {
        // prevent instantiation
    }

    @SuppressWarnings("unchecked")
    public static <T> Flowable<T> reverse(Flowable<T> source) {
        return source.toList().toFlowable()
                .flatMap((Function<List<T>, Flowable<T>>) (Function<?, ?>) REVERSE_LIST);
    }

    private static final Function<List<Object>, Flowable<Object>> REVERSE_LIST = new Function<List<Object>, Flowable<Object>>() {
        @Override
        public Flowable<Object> apply(List<Object> list) {
            return Flowable.fromIterable(reverse(list));
        }
    };

    private static <T> Iterable<T> reverse(final List<T> list) {
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {

                    int i = list.size();

                    @Override
                    public boolean hasNext() {
                        return i > 0;
                    }

                    @Override
                    public T next() {
                        i--;
                        return list.get(i);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                };
            }
        };
    }

}
