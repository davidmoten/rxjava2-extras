package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.github.davidmoten.rx2.Actions;
import com.github.davidmoten.rx2.FlowableTransformers;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subscribers.TestSubscriber;

public class FlowableDoOnEmptyTest {

    @Test
    public void testNonEmpty() {
        Flowable<String> source = Flowable.just("Chicago", "Houston", "Phoenix");

        final AtomicBoolean wasCalled = new AtomicBoolean(false);

        source.compose(FlowableTransformers.<String>doOnEmpty(new Action() {
            @Override
            public void run() {
                wasCalled.set(true);
            }
        })).subscribe();

        assertFalse(wasCalled.get());
    }

    @Test
    public void testEmpty() {
        Flowable<String> source = Flowable.empty();

        final AtomicBoolean wasCalled = new AtomicBoolean(false);

        source.compose(FlowableTransformers.<String>doOnEmpty(new Action() {
            @Override
            public void run() {
                wasCalled.set(true);
            }
        })).subscribe();

        assertTrue(wasCalled.get());
    }

    @Test
    public void testCancel() {
        final AtomicBoolean wasCalled = new AtomicBoolean(false);

        PublishSubject<Integer> subject = PublishSubject.create();

        Disposable disposable = subject.toFlowable(BackpressureStrategy.BUFFER) //
                .compose(FlowableTransformers.<Integer>doOnEmpty(new Action() {
                    @Override
                    public void run() {
                        wasCalled.set(true);
                    }
                })).take(3).subscribe();

        assertTrue(subject.hasObservers());

        subject.onNext(0);
        subject.onNext(1);

        assertTrue(subject.hasObservers());

        subject.onNext(2);

        assertFalse(subject.hasObservers());

        disposable.dispose();

        assertFalse(wasCalled.get());
    }

    @Test
    public void testBackPressure() {
        final AtomicBoolean wasCalled = new AtomicBoolean(false);
        Flowable //
                .range(0, 1000) //
                .compose(FlowableTransformers.<Integer>doOnEmpty(Actions.setToTrue(wasCalled))) //
                .test(0) //
                .requestMore(1) //
                .assertValueCount(1) //
                .assertNotTerminated();
        assertFalse(wasCalled.get());
    }

    @Test
    public void subscriberStateTest() {
        // final AtomicInteger counter = new AtomicInteger(0);
        //
        // final AtomicInteger callCount = new AtomicInteger(0);
        //
        // Flowable<Integer> o = Flowable.defer(new Func0<Flowable<Integer>>() {
        // @Override
        // public Flowable<Integer> call() {
        // return Flowable.range(1, counter.getAndIncrement() % 2);
        // }
        // }).compose(Transformers.<Integer>doOnEmpty(Actions.increment0(callCount)));
        //
        // o.subscribe();
        // o.subscribe();
        // o.subscribe();
        // o.subscribe();
        // o.subscribe();
        //
        // assert (callCount.get() == 3);
    }

    @Test
    public void ifSourceEmitsErrorThenDoOnEmptyIsNotRun() {
        AtomicBoolean set = new AtomicBoolean(false);
        Exception ex = new Exception("boo");
        Flowable.error(ex) //
                .compose(FlowableTransformers.doOnEmpty(Actions.setToTrue(set))) //
                .test().assertError(ex).assertNoValues();
        assertFalse(set.get());
    }

    @Test
    public void ifOnEmptyActionThrowsExceptionThenSubscribeThrows() {
        Flowable.empty() //
                .compose(FlowableTransformers.doOnEmpty(Actions.throwing(new SQLException()))) //
                .test() //
                .assertNoValues() //
                .assertError(SQLException.class);
    }

    @Test
    public void ifOnEmptyActionThrowsNonFatalRuntimeExceptionThenErrorEmitted() {
        Flowable.empty() //
                .compose(FlowableTransformers.doOnEmpty(Actions.throwing(new NumberFormatException()))) //
                .test() //
                .assertNoValues() //
                .assertError(NumberFormatException.class);
    }

    @Test
    public void testUnsubscribeAfterActionButBeforeCompletionDoesNotAffectCompletion() {
        final TestSubscriber<Object> ts = TestSubscriber.create();
        Flowable.empty() //
                .compose(FlowableTransformers.doOnEmpty(new Action() {
                    @Override
                    public void run() {
                        ts.cancel();
                    }
                })).subscribe(ts);
        ts.assertNoValues();
        ts.assertComplete();
    }

}
