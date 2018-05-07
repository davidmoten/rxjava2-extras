package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.reactivestreams.Publisher;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import rx.Observable;
import rx.Observable.Transformer;
import rx.observables.StringObservable;

@State(Scope.Benchmark)
public class Benchmarks {

    static final String lines = create(10000, 100);
    static final Flowable<String> source = Flowable.just(lines, lines, lines, lines, lines);

    @Benchmark
    public String splitStandardTake5() {
        return source.compose(Strings.split("\n")).take(5).blockingLast();
    }

    @Benchmark
    public String splitStandardWithPatternTake5() {
        return source.compose(Strings.split(Pattern.compile("\n"))).take(5).blockingLast();
    }

    @Benchmark
    public String splitSimpleTake5() {
        return source.compose(Strings.splitSimple("\n")).take(5).blockingLast();
    }

    @Benchmark
    public String splitStandard() {
        return source.compose(Strings.split("\n")).blockingLast();
    }

    @Benchmark
    public String splitStandardWithPattern() {
        return source.compose(Strings.split(Pattern.compile("\n"))).blockingLast();
    }

    @Benchmark
    public String splitSimple() {
        return source.compose(Strings.splitSimple("\n")).blockingLast();
    }

    private static Flowable<Integer> range = Flowable
            .defer(new Callable<Publisher<? extends Integer>>() {
                final int[] count = new int[1];

                @Override
                public Publisher<? extends Integer> call() throws Exception {
                    return Flowable.generate(new Consumer<Emitter<Integer>>() {
                        @Override
                        public void accept(Emitter<Integer> emitter) throws Exception {
                            count[0]++;
                            emitter.onNext(count[0]);
                            if (count[0] == 1000) {
                                emitter.onComplete();
                            }
                        }
                    });
                }
            });

    @Benchmark
    public Long mergeTwoStreams() {
        return Flowable.merge(Flowable.just(range, range, range)).count().blockingGet();
    }

    @Benchmark
    public Long mergeTwoStreamsInterleaved() {
        return Flowables.mergeInterleaved(Flowable.just(range, range, range)) //
                .maxConcurrency(4) //
                .batchSize(128) //
                .build() //
                .count() //
                .blockingGet();
    }

    @Benchmark
    public String splitRxJavaStringTake5() {
        return Observable.just(lines).compose(new Transformer<String, String>() {
            @Override
            public Observable<String> call(Observable<String> o) {
                return StringObservable.split(o, "\n");
            }
        }).take(5).last().toBlocking().last();
    }

    @Benchmark
    public String splitRxJavaString() {
        return Observable.just(lines).compose(new Transformer<String, String>() {
            @Override
            public Observable<String> call(Observable<String> o) {
                return StringObservable.split(o, "\n");
            }
        }).last().toBlocking().last();
    }

    private static String create(int lines, int lineLength) {
        StringBuilder s = new StringBuilder(lines * lineLength);
        for (int i = 0; i < lines; i++) {
            for (int j = 0; j < lineLength; j++) {
                s.append((char) (48 + (i * 137 + j) % 74));
            }
            s.append("\n");
        }
        return s.toString();
    }

    public static void main(String[] args) {
        range.doOnNext(Consumers.println()).subscribe();
    }
}