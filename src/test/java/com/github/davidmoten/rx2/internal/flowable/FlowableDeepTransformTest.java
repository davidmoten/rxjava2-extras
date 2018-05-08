package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

public class FlowableDeepTransformTest {

    @Test
    @Ignore
    public void test() {
        Flowable<String> source = Flowable.just("a", "b", "c", "d", "e", "f", "g");
        Flowables.deepTransform(source, new BiFunction<Flowable<String>, Runnable, Flowable<String>>() {
            @Override
            public Flowable<String> apply(final Flowable<String> f, final Runnable b) throws Exception {
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
