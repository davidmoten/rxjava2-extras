package com.github.davidmoten.rx2.internal.flowable;


import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;

public final class CachedFlowable<T> extends Flowable<T> {

    private final OnSubscribeCacheResettable<T> cache;

    public CachedFlowable(Flowable<T> source) {
        this(new OnSubscribeCacheResettable<T>(source));
    }

    CachedFlowable(OnSubscribeCacheResettable<T> cache) {
        this.cache = cache;
    }

    public CachedFlowable<T> reset() {
        cache.reset();
        return this;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> subscriber) {
        cache.subscribe(subscriber);
    }
}