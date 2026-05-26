package com.github.davidmoten.rx2;

import io.reactivex.Flowable;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Consumer;
import io.reactivex.schedulers.Schedulers;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.flowable.CloseableFlowableWithReset;

import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class FlowablesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Flowables.class);
    }


    @Test
    public void testCache() {

        final AtomicInteger subscriptionCount = new AtomicInteger(0);

        Flowable<String> source =
                Flowable.just("Alpha", "Beta", "Gamma", "Delta", "Epsilon");

        final CloseableFlowableWithReset<List<String>> closeable = Flowables.cache(
                source.doOnSubscribe(new Consumer<Subscription>() {
                                         @Override
                                         public void accept(@NonNull Subscription subscription) throws Exception {
                                             subscriptionCount.incrementAndGet();
                                         }
                                     }
                ).toList().toFlowable(), 3, TimeUnit.SECONDS, Schedulers.computation());

        Flowable<List<String>> timed = closeable.flowable().doOnNext(new Consumer<List<String>>() {
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
            Thread.sleep(4500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        timed.subscribe();

        assertTrue(subscriptionCount.get() == 2);
    }
    
}
