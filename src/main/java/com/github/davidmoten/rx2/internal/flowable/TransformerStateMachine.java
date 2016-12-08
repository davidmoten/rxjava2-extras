package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.FlowableTransformer;
import io.reactivex.Notification;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Cancellable;
import io.reactivex.functions.Function;
import io.reactivex.functions.Function3;
import io.reactivex.functions.Predicate;

public final class TransformerStateMachine<State, In, Out> implements FlowableTransformer<In, Out> {

    private final Callable<? extends State> initialState;
    private final Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition;
    private final BiPredicate<? super State, ? super FlowableEmitter<Out>> completion;
    private final BackpressureStrategy backpressureStrategy;
    private final int requestBatchSize;

    private TransformerStateMachine(Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            BackpressureStrategy backpressureStrategy, int requestBatchSize) {
        Preconditions.checkNotNull(initialState);
        Preconditions.checkNotNull(transition);
        Preconditions.checkNotNull(completion);
        Preconditions.checkNotNull(backpressureStrategy);
        Preconditions.checkArgument(requestBatchSize > 0,
                "initialRequest must be greater than zero");
        this.initialState = initialState;
        this.transition = transition;
        this.completion = completion;
        this.backpressureStrategy = backpressureStrategy;
        this.requestBatchSize = requestBatchSize;
    }

    public static <State, In, Out> FlowableTransformer<In, Out> create(
            Callable<? extends State> initialState,
            Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            BackpressureStrategy backpressureStrategy, int requestBatchSize) {
        return new TransformerStateMachine<State, In, Out>(initialState, transition, completion,
                backpressureStrategy, requestBatchSize);
    }

    @Override
    public Flowable<Out> apply(final Flowable<In> source) {
        // use defer so we can have a single state reference for each
        // subscription
        return Flowable.defer(new Callable<Flowable<Out>>() {
            @Override
            public Flowable<Out> call() throws Exception {
                Mutable<State> state = new Mutable<State>(initialState.call());
                return source.materialize()
                        // do state transitions and emit notifications
                        // use flatMap to emit notification values
                        .flatMap(execute(transition, completion, state, backpressureStrategy),
                                requestBatchSize)
                        // complete if we encounter an unsubscribed sentinel
                        .takeWhile(NOT_UNSUBSCRIBED)
                        // flatten notifications to a stream which will enable
                        // early termination from the state machine if desired
                        .dematerialize();
            }
        });
    }

    private static <State, Out, In> Function<Notification<In>, Flowable<Notification<Out>>> execute(
            final Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
            final BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
            final Mutable<State> state, final BackpressureStrategy backpressureStrategy) {

        return new Function<Notification<In>, Flowable<Notification<Out>>>() {

            @Override
            public Flowable<Notification<Out>> apply(final Notification<In> in) {

                return Flowable.create(new FlowableOnSubscribe<Notification<Out>>() {

                    @Override
                    public void subscribe(FlowableEmitter<Notification<Out>> emitter)
                            throws Exception {
                        FlowableEmitter<Out> w = wrap(emitter);
                        if (in.isOnNext()) {
                            state.value = transition.apply(state.value, in.getValue(), w);
                            if (!emitter.isCancelled())
                                emitter.onComplete();
                            else {
                                // this is a special emission to indicate that
                                // the transition called unsubscribe. It will be
                                // filtered later.
                                emitter.onNext(UnsubscribedNotificationHolder
                                        .<Out> unsubscribedNotification());
                            }
                        } else if (in.isOnComplete()) {
                            if (completion.test(state.value, w) && !emitter.isCancelled()) {
                                w.onComplete();
                            }
                        } else if (!emitter.isCancelled()) {
                            w.onError(in.getError());
                        }
                    }

                }, backpressureStrategy);
            }
        };
    }

    private static final class UnsubscribedNotificationHolder {
        private static final Notification<Object> INSTANCE = Notification
                .createOnNext(new Object());

        @SuppressWarnings("unchecked")
        static <T> Notification<T> unsubscribedNotification() {
            return (Notification<T>) INSTANCE;
        }
    }

    private static final Predicate<Notification<?>> NOT_UNSUBSCRIBED = new Predicate<Notification<?>>() {

        @Override
        public boolean test(Notification<?> t) {
            return t != UnsubscribedNotificationHolder.unsubscribedNotification();
        }

    };

    private static final class Mutable<T> {
        T value;

        Mutable(T value) {
            this.value = value;
        }
    }

    private static <Out> NotificationEmitter<Out> wrap(
            FlowableEmitter<? super Notification<Out>> emitter) {
        return new NotificationEmitter<Out>(emitter);
    }

    private static final class NotificationEmitter<Out> implements FlowableEmitter<Out> {

        private final FlowableEmitter<? super Notification<Out>> emitter;

        NotificationEmitter(FlowableEmitter<? super Notification<Out>> emitter) {
            this.emitter = emitter;
        }

        @Override
        public void onComplete() {
            emitter.onNext(Notification.<Out> createOnComplete());
        }

        @Override
        public void onError(Throwable e) {
            emitter.onNext(Notification.<Out> createOnError(e));
        }

        @Override
        public void onNext(Out t) {
            emitter.onNext(Notification.createOnNext(t));
        }

        @Override
        public void setDisposable(Disposable s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCancellable(Cancellable c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long requested() {
            return emitter.requested();
        }

        @Override
        public boolean isCancelled() {
            return emitter.isCancelled();

        }

        @Override
        public FlowableEmitter<Out> serialize() {
            throw new UnsupportedOperationException();
        }

    }

}
