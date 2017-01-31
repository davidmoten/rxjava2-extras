package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.rx2.Actions;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.StateMachine.Completion2;
import com.github.davidmoten.rx2.StateMachine.Emitter;
import com.github.davidmoten.rx2.StateMachine.Errored;
import com.github.davidmoten.rx2.StateMachine.Transition2;
import com.github.davidmoten.rx2.StateMachine2;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Burst;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.Consumer;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;

public final class FlowableStateMachineTest {

    private static final Transition2<String, Integer, Integer> PASS_THROUGH_TRANSITION = new Transition2<String, Integer, Integer>() {

        @Override
        public String apply(String state, Integer value, Emitter<Integer> emitter) {
            emitter.onNext_(value);
            return state;
        }
    };

    private static FlowableTransformer<Integer, Integer> passThrough(int batchSize) {

        return StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .requestBatchSize(batchSize) //
                .build();
    }

    @Test
    public void testRequestBatchSize1() {
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(passThrough(1)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testPassThroughWithCustomCompletion() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                        emitter.onComplete_();
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(sm) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testPassThroughEmitterCompletesTwice() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                        emitter.onComplete_();
                        emitter.onComplete_();
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(sm) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testPassThroughEmitterOnNextAfterCompletion() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                        emitter.onComplete_();
                        emitter.onNext_(8);
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(sm) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testPassThroughEmitterErrorAfterCompletion() {
        List<Throwable> list = new CopyOnWriteArrayList<Throwable>();
        try {
            RxJavaPlugins.setErrorHandler(Consumers.addTo(list));
            FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                    .initialState("") //
                    .transition(PASS_THROUGH_TRANSITION) //
                    .completion(new Completion2<String, Integer>() {
                        @Override
                        public void accept(String state, Emitter<Integer> emitter) {
                            emitter.onComplete_();
                            emitter.onError_(new ThrowingException());
                        }
                    }) //
                    .requestBatchSize(1) //
                    .build();

            Flowable.just(1, 2, 3, 4, 5, 6) //
                    .compose(sm) //
                    .test() //
                    .assertValues(1, 2, 3, 4, 5, 6) //
                    .assertComplete();
            assertEquals(1, list.size());
        } finally {
            RxJavaPlugins.reset();
        }

    }

    @Test
    public void testCompletionThrows() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .completion(new Completion2<String, Integer>() {
                    @Override
                    public void accept(String state, Emitter<Integer> emitter) {
                        throw new ThrowingException();
                    }
                }) //
                .requestBatchSize(1) //
                .build();
        Flowable.just(1) //
                .compose(sm) //
                .test() //
                .assertValues(1) //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testRequestBatchSize2() {
        Flowable.just(1, 2, 3, 4, 5, 6) //
                .compose(passThrough(2)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6) //
                .assertComplete();
    }

    @Test
    public void testErrorPassThrough() {
        RuntimeException e = new RuntimeException();
        Flowable.<Integer> error(e) //
                .compose(passThrough(1)) //
                .test() //
                .assertNoValues() //
                .assertError(e);
    }

    @Test
    public void testRequestBatchSize1WithBackpressure() {
        Flowable.just(1, 2) //
                .compose(passThrough(1)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValues(1, 2) //
                .requestMore(1) //
                .assertComplete();
    }

    @Test
    public void testRequestBatchSize1WithBackpressureRequestMoreThanAvailable() {
        Flowable.just(1, 2) //
                .compose(passThrough(1)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(100) //
                .assertValues(1, 2) //
                .assertComplete();
    }

    @Test
    public void testRequestBatchSize2WithBackpressure() {
        Flowable.just(1, 2) //
                .compose(passThrough(2)) //
                .test(1) //
                .assertValues(1) //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValues(1, 2) //
                .requestMore(1) //
                .assertComplete();
    }

    @Test
    public void testStateFactoryReturnsNull() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialStateFactory(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return null;
                    }
                }) //
                .transition(PASS_THROUGH_TRANSITION) //
                .build();
        Flowable.just(1) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testStateFactoryReturnsNullFromErrorStream() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialStateFactory(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return null;
                    }
                }) //
                .transition(PASS_THROUGH_TRANSITION) //
                .build();
        Flowable.<Integer> error(new ThrowingException()) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void testOnNextThrowsWithBurstSource() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(new Transition2<String, Integer, Integer>() {

                    @Override
                    public String apply(String state, Integer value, Emitter<Integer> emitter) {
                        throw new ThrowingException();
                    }
                }) //
                .requestBatchSize(10) //
                .build();
        Burst.items(1, 2, 3).create() //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }
    
    @Test
    public void testCancelFromTransition() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(new Transition2<String, Integer, Integer>() {

                    @Override
                    public String apply(String state, Integer value, Emitter<Integer> emitter) {
                       emitter.cancel_();
                       return state;
                    }
                }) //
                .requestBatchSize(10) //
                .build();
        Burst.items(1, 2, 3).create() //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertNotTerminated();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRequestBatchSize() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .requestBatchSize(-10) //
                .build();
        Flowable.just(1).compose(sm).test();
    }

    @Test
    public void testOnNextThrowsWithBurstSourceThatTerminatesWithError() {
        List<Throwable> list = new CopyOnWriteArrayList<Throwable>();
        try {
            RxJavaPlugins.setErrorHandler(Consumers.addTo(list));
            FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                    .initialState("") //
                    .transition(new Transition2<String, Integer, Integer>() {

                        @Override
                        public String apply(String state, Integer value, Emitter<Integer> emitter) {
                            throw new ThrowingException();
                        }
                    }) //
                    .requestBatchSize(10) //
                    .build();
            Burst.item(1).error(new RuntimeException()) //
                    .compose(sm) //
                    .test() //
                    .assertNoValues() //
                    .assertError(ThrowingException.class);
            assertEquals(1, list.size());
        } finally {
            RxJavaPlugins.reset();
        }
    }

    @Test
    public void testStateFactoryReturnsNullOnEmptySource() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialStateFactory(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        return null;
                    }
                }) //
                .transition(PASS_THROUGH_TRANSITION) //
                .build();
        Flowable.<Integer> empty() //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(NullPointerException.class);
    }

    @Test
    public void errorActionThrows() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .errored(new Errored<String, Integer>() {
                    @Override
                    public void accept(String state, Throwable error, Emitter<Integer> emitter) {
                        throw new ThrowingException();
                    }
                }) //
                .build();
        Flowable.<Integer> error(new RuntimeException()) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void errorActionPassThrough() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(PASS_THROUGH_TRANSITION) //
                .errored(new Errored<String, Integer>() {
                    @Override
                    public void accept(String state, Throwable error, Emitter<Integer> emitter) {
                        emitter.onError_(error);
                    }
                }) //
                .build();
        Flowable.<Integer> error(new ThrowingException()) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertError(ThrowingException.class);
    }

    @Test
    public void testAsync() {
        final AtomicInteger n = new AtomicInteger(0);
        final int N = 1000000;
        Flowable.range(1, N) //
                .compose(passThrough(1)) //
                .observeOn(Schedulers.computation()) //
                .rebatchRequests(100) //
                .doOnNext(new Consumer<Integer>() {
                    @Override
                    public void accept(Integer value) throws Exception {
                        if (!n.compareAndSet(value - 1, value)) {
                            throw new RuntimeException();
                        }
                    }
                }) //
                .test() //
                .awaitDone(60, TimeUnit.SECONDS) //
                .assertComplete();
        assertEquals(N, n.get());
    }

    @Test
    public void testCancel() {
        final AtomicBoolean terminated = new AtomicBoolean();
        final List<Integer> list = new CopyOnWriteArrayList<Integer>();
        Flowable.just(1, 2) //
                .compose(passThrough(1)) //
                .doOnComplete(Actions.setToTrue(terminated)) //
                .subscribe(new Subscriber<Integer>() {

                    Subscription parent;

                    @Override
                    public void onComplete() {
                        terminated.set(true);
                    }

                    @Override
                    public void onError(Throwable e) {
                        terminated.set(true);
                    }

                    @Override
                    public void onNext(Integer t) {
                        list.add(t);
                        parent.cancel();
                    }

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.parent = s;
                        parent.request(Long.MAX_VALUE);
                    }
                });
        assertEquals(Arrays.asList(1), list);
        assertFalse(terminated.get());
    }

    @Test
    public void noActionTransition() {
        FlowableTransformer<Integer, Integer> sm = StateMachine2.builder() //
                .initialState("") //
                .transition(new Transition2<String, Integer, Integer>() {
                    @Override
                    public String apply(String state, Integer value, Emitter<Integer> emitter) {
                        return state;
                    }
                }) //
                .build();
        Flowable.just(1, 2) //
                .compose(sm) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }
}
