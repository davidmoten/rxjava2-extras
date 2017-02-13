package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.LongConsumer;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.subjects.ReplaySubject;
import io.reactivex.subjects.Subject;

public final class FlowableFetchPagesByRequest {

    private FlowableFetchPagesByRequest() {
        // prevent instantiation
    }

    public static <T> Flowable<T> create(final BiFunction<? super Long, ? super Long, ? extends Flowable<T>> fetch,
            final long start, final int maxConcurrency) {
        return Flowable.defer(new Callable<Flowable<T>>() {
            @Override
            public Flowable<T> call() throws Exception {
                // need a ReplaySubject because requests can come through before
                // concatEager has established subscriptions to the subject
                final ReplaySubject<Flowable<T>> subject = ReplaySubject.create();
                final AtomicLong position = new AtomicLong(start);
                LongConsumer request = new LongConsumer() {
                    @Override
                    public void accept(final long n) throws Exception {
                        final long pos = position.getAndAdd(n);
                        if (SubscriptionHelper.validate(n)) {
                            Flowable<T> flowable;
                            try {
                                flowable = fetch.apply(pos, n);
                            } catch (Throwable e) {
                                Exceptions.throwIfFatal(e);
                                subject.onError(e);
                                return;
                            }
                            flowable = notifySubjectOfCompletionIfFullRequestNotReturned(flowable, subject, n);
                            subject.onNext(flowable);
                        }
                    }

                };
                return Flowable //
                        .concatEager(subject.serialize() //
                                .toFlowable(BackpressureStrategy.BUFFER), maxConcurrency, 128) //
                        .doOnRequest(request);
            }
        });
    }

    private static <T> Flowable<T> notifySubjectOfCompletionIfFullRequestNotReturned(final Flowable<T> flowable,
            final Subject<Flowable<T>> subject, final long n) {
        return Flowable.defer(new Callable<Publisher<T>>() {
            long count;

            @Override
            public Flowable<T> call() throws Exception {
                return flowable.doOnNext(new Consumer<T>() {

                    @Override
                    public void accept(T t) throws Exception {
                        count++;
                    }
                }).doOnComplete(new Action() {

                    @Override
                    public void run() throws Exception {
                        if (count < n) {
                            subject.onComplete();
                        }
                    }
                });
            }
        });

    }

}
