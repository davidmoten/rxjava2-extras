package com.github.davidmoten.rx2;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.observable.CloseableObservableWithReset;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class ObservablesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Observables.class);
    }

    @Test
    public void testCache() {

        final AtomicInteger subscriptionCount = new AtomicInteger(0);

        Flowable<String> source = Flowable.just("Alpha", "Beta", "Gamma", "Delta", "Epsilon");

        final CloseableObservableWithReset<List<String>> closeable = Observables
                .cache(source.doOnSubscribe(new Consumer<Subscription>() {
                    @Override
                    public void accept(@NonNull Subscription subscription) throws Exception {
                        subscriptionCount.incrementAndGet();
                    }
                }).toList().toObservable(), 3, TimeUnit.SECONDS, Schedulers.computation());

        Observable<List<String>> timed = closeable.observable()
                .doOnNext(new Consumer<List<String>>() {
                    @Override
                    public void accept(@NonNull List<String> s) throws Exception {
                        closeable.reset();
                    }
                });

        timed.subscribe();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        timed.subscribe();
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        timed.subscribe();

        assertTrue(subscriptionCount.get() == 2);

        // TODO assert stuff about closing
        // closeable.close();
        // closeable.close();
    }

}
