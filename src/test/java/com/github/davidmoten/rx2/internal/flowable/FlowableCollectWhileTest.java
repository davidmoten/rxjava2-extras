package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.rx2.BiFunctions;
import com.github.davidmoten.rx2.BiPredicates;
import com.github.davidmoten.rx2.Callables;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Burst;
import com.github.davidmoten.rx2.flowable.Transformers;
import com.google.common.collect.Lists;

import io.reactivex.Flowable;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.subscribers.TestSubscriber;

public final class FlowableCollectWhileTest {

    private static final BiPredicate<List<Integer>, Integer> BUFFER_TWO = new BiPredicate<List<Integer>, Integer>() {

        @Override
        public boolean test(List<Integer> list, Integer t) throws Exception {
            return list.size() <= 1;
        }
    };

    @Test
    public void testEmpty() {
        Flowable.<Integer>empty() //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }

    @Test
    public void testOne() {
        Flowable.just(3) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test() //
                .assertValue(Lists.newArrayList(3)) //
                .assertComplete();
    }

    @Test
    public void testTwo() {
        Flowable.just(3, 4) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test() //
                .assertValue(Lists.newArrayList(3, 4)) //
                .assertComplete();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThree() {
        Flowable.just(3, 4, 5) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test() //
                .assertValues(Lists.newArrayList(3, 4), Lists.newArrayList(5)) //
                .assertComplete();
    }

    @Test
    public void testFactoryReturnsNullShouldEmitNPE() {
        Flowable.just(3) //
                .compose(Transformers. //
                        collectWhile(Callables.<List<Integer>>toNull(), BiFunctions.constant(new ArrayList<Integer>()),
                                BUFFER_TWO)) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testAddReturnsNullShouldEmitNPE() {
        Flowable.just(3) //
                .compose(Transformers. //
                        collectWhile(Callables.<List<Integer>>toNull(),
                                BiFunctions.<List<Integer>, Integer, List<Integer>>toNull(), BUFFER_TWO)) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testAddReturnsNull() {
        Flowable.just(3) //
                .compose(Transformers. //
                        collectWhile( //
                                Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()),
                                BiFunctions.<List<Integer>, Integer, List<Integer>>toNull(), //
                                BUFFER_TWO)) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testAddThrows() {
        Flowable.just(3) //
                .compose(Transformers. //
                        collectWhile( //
                                Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()),
                                BiFunctions.<List<Integer>, Integer, List<Integer>>throwing(), //
                                BUFFER_TWO)) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testConditionThrows() {
        Flowable.just(3) //
                .compose(Transformers. //
                        collectWhile( //
                                Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
                                BiPredicates.throwing())) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testDoesNotEmitAfterErrorInOnNextIfUpstreamDoesNotHonourCancellationImmediately() {
        Burst.items(1, 2).create() //
                .compose(Transformers. //
                        collectWhile( //
                                Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
                                BiPredicates.throwing())) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testDoesNotTwoErrorsIfUpstreamDoesNotHonourCancellationImmediately() {
        try {
            List<Throwable> list = new CopyOnWriteArrayList<Throwable>();
            RxJavaPlugins.setErrorHandler(Consumers.addTo(list));
            Burst.items(1).error(new ThrowingException())//
                    .compose(Transformers. //
                            collectWhile( //
                                    Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
                                    BiPredicates.throwing())) //
                    .test() //
                    .assertNoValues() //
                    .assertError(ThrowingException.class);
            assertEquals(1, list.size());
            System.out.println(list.get(0));
            assertTrue(list.get(0) instanceof UndeliverableException);
            assertTrue(list.get(0).getCause() instanceof ThrowingException);
        } finally {
            RxJavaPlugins.reset();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBackpressure() {
        Flowable.just(3, 4, 5, 6, 7, 8) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test(1) //
                .assertValue(list(3, 4)) //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValues(list(3, 4), list(5, 6)) //
                .assertNotTerminated() //
                .requestMore(2) //
                .assertValues(list(3, 4), list(5, 6), list(7, 8)) //
                .assertComplete();
    }

    @Test
    public void testBackpressureAndCancel() {

        TestSubscriber<List<Integer>> ts = Flowable.just(3, 4, 5, 6, 7, 8) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .test(1) //
                .assertValue(list(3, 4)) //
                .assertNotTerminated();
        ts.cancel(); //
        ts.requestMore(Long.MAX_VALUE) //
                .assertValueCount(1) //
                .assertNotTerminated();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRequestWhileProcessingOnNext() {
        final List<List<Integer>> list = new ArrayList<List<Integer>>();
        Subscriber<List<Integer>> subscriber = new Subscriber<List<Integer>>() {

            private Subscription parent;

            @Override
            public void onSubscribe(Subscription s) {
                this.parent = s;
                parent.request(1);
            }

            @Override
            public void onNext(List<Integer> a) {
                list.add(a);
                parent.request(1);
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onComplete() {

            }
        };
        Flowable.just(3, 4, 5, 6) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .subscribe(subscriber);
        assertEquals(list, Arrays.asList(list(3, 4), list(5, 6)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCancelWhileProcessingOnNext() {
        final List<List<Integer>> list = new ArrayList<List<Integer>>();
        Subscriber<List<Integer>> subscriber = new Subscriber<List<Integer>>() {

            private Subscription parent;

            @Override
            public void onSubscribe(Subscription s) {
                this.parent = s;
                parent.request(2);
            }

            @Override
            public void onNext(List<Integer> a) {
                list.add(a);
                parent.cancel();
            }

            @Override
            public void onError(Throwable e) {
            }

            @Override
            public void onComplete() {

            }
        };
        Flowable.just(3, 4, 5, 6) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO)) //
                .subscribe(subscriber);
        assertEquals(Arrays.asList(list(3, 4)), list);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWhenEmitRemainderFalse() {
        Flowable.just(3, 4, 5) //
                .compose(Transformers. //
                        toListWhile(BUFFER_TWO, false)) //
                .test() //
                .assertValues(Lists.newArrayList(3, 4)) //
                .assertComplete();
    }

    private static List<Integer> list(Integer... values) {
        return Lists.newArrayList(values);
    }

    private static final BiFunction<List<Integer>, Integer, List<Integer>> ADD = new BiFunction<List<Integer>, Integer, List<Integer>>() {

        @Override
        public List<Integer> apply(List<Integer> list, Integer t) throws Exception {
            list.add(t);
            return list;
        }
    };

}
