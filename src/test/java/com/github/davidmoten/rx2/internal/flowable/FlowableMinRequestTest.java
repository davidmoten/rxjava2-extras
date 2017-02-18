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
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;

public class FlowableMinRequestTest {

    @Test
    public void testConstrainedFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.minRequest(2)) //
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
                .compose(Transformers.minRequest(1, 2)) //
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
                .compose(Transformers.minRequest(2)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(Long.MAX_VALUE), requests);
    }

    @Test
    public void testMinWhenRequestIsMaxValueAndSourceDoesNotComplete() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .concatWith(Flowable.<Integer>never()) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.minRequest(2)) //
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
                .compose(Transformers.minRequest(2)) //
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
        Flowable.<Long>error(new ThrowingException()) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.minRequest(2)) //
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
                .compose(Transformers.<Integer>minRequest(2)) //
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

    @Test
    public void testRebatchRequestsMinEqualsMaxDontConstrainFirstUsesRxJavaCoreRebatchRequests() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.rebatchRequests(5, 5)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(5L, 4L, 4L), requests);
    }

    @Test
    public void testRebatchRequestsMinEqualsMaxDontConstrainFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.rebatchRequests(5, 5, false)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(5L, 5L), requests);
    }

    @Test
    public void testRebatchRequestsMinLessThanMax() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(Transformers.rebatchRequests(2, 3, false)) //
                .test(10) //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(3L, 3L, 3L, 2L), requests);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRebatchRequestsMinMoreThanMaxThrows() {
        Transformers.rebatchRequests(3, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMinNegativeThrows() {
        Flowable.just(1).compose(Transformers.minRequest(0));
    }

}
