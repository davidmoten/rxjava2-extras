package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

public class FlowableDeepTransformTest {

    @Test
    public void test() {
        Flowable<String> source = Flowable.just("a", "b", "c", "d", "e", "f", "g");
        Flowables.deepTransform(source, new BiFunction<Flowable<String>, AtomicBoolean, Flowable<String>>() {
            @Override
            public Flowable<String> apply(final Flowable<String> f, final AtomicBoolean b) throws Exception {
                return f.buffer(2) //
                        .map(new Function<List<String>, String>() {
                            @Override
                            public String apply(List<String> x) throws Exception {
                                return x.size() == 2 ? x.get(0) + x.get(1) : x.get(0);
                            }
                        });
            }
        });
    }

}
