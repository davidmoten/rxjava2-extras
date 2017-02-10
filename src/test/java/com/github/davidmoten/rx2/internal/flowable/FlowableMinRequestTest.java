package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;
import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.Flowable;

public class FlowableMinRequestTest {

    @Test
    public void testConstrainedFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .rebatchRequests(1) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(2L, 2L, 2L, 2L, 2L), requests);
    }

    @Test
    public void testUnconstrainedFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, false)) //
                .rebatchRequests(1) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(1L, 2L, 2L, 2L, 2L, 2L), requests);
    }

    @Test
    public void testMinWhenRequestIsMaxValue() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), requests);
    }

    @Test
    public void testMinWhenRequestIsMaxValueAndSourceDoesNotComplete() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .concatWith(Flowable.<Integer> never()) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertNotComplete();
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), requests);
    }

    @Test
    public void testBackpressure() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .test(1) //
                .assertValues(1) //
                .requestMore(9) //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(2L, 8L), requests);
    }

    @Test
    public void testError() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.<Long> error(new ThrowingException()) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), requests);
    }

    @Test
    public void testCancel() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        final List<Integer> values = new CopyOnWriteArrayList<Integer>();
        final AtomicBoolean terminated = new AtomicBoolean();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.<Integer> minRequest(2, true)) //
                .subscribe(new Subscriber<Integer>() {

                    private Subscription parent;

                    @Override
                    public void onSubscribe(Subscription parent) {
                        this.parent = parent;
                        parent.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Integer t) {
                        values.add(t);
                        parent.cancel();
                    }

                    @Override
                    public void onError(Throwable t) {
                        terminated.set(true);
                    }

                    @Override
                    public void onComplete() {
                        terminated.set(true);
                    }
                });
        assertEquals(Lists.newArrayList(1), values);
        assertFalse(terminated.get());
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), requests);
    }

}