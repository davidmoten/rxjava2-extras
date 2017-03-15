package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.functions.Function;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.subjects.PublishSubject;

public class FlowableReduce<T> extends Flowable<T> {

    private final Flowable<T> source;
    private Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private int depth;

    public FlowableReduce(Flowable<T> source,
            Function<? super Flowable<T>, ? extends Flowable<T>> reducer, int depth) {
        Preconditions.checkArgument(depth > 0, "depth must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.depth = depth;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        PublishSubject<T>[] subjects = new PublishSubject[depth];
        source.subscribe(new FlowableReduceSubscriber<T>(reducer, depth, subjects, 0, child));
    }

    private static final class FlowableReduceSubscriber<T>
            implements FlowableSubscriber<T>, Subscription {

        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final int depth;
        private final Subscriber<? super T> child;

        private final PublishSubject<T>[] subjects;
        private final int index;

        private Subscription parent;

        @SuppressWarnings("unchecked")
        FlowableReduceSubscriber(Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
                int depth, PublishSubject<T>[] subjects, int index, Subscriber<? super T> child) {
            this.reducer = reducer;
            this.depth = depth;
            this.subjects = subjects;
            this.index = index;
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
            }
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                // TODO
            }
        }

        @Override
        public void onNext(T t) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onError(Throwable t) {
            // TODO Auto-generated method stub

        }

        @Override
        public void onComplete() {
            // TODO Auto-generated method stub

        }

        @Override
        public void cancel() {
            // TODO Auto-generated method stub

        }

    }

}
