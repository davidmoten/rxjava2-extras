package com.github.davidmoten.rx2.observable;

import com.github.davidmoten.rx2.internal.observable.OnSubscribeCacheResetable;

import io.reactivex.Observable;
import io.reactivex.Observer;

public final class CachedObservable<T> extends Observable<T> {

    private final OnSubscribeCacheResetable<T> cache;

    public CachedObservable(Observable<T> source) {
        this(new OnSubscribeCacheResetable<T>(source));
    }

    CachedObservable(OnSubscribeCacheResetable<T> cache) {
        this.cache = cache;
    }

    public CachedObservable<T> reset() {
        cache.reset();
        return this;
    }

    @Override
    protected void subscribeActual(Observer<? super T> observer) {
        cache.subscribe(observer);
    }

}