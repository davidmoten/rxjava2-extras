package com.github.davidmoten.rx2.observable;


import io.reactivex.Observable;
import io.reactivex.Observer;


public final class CachedObservable<T> extends Observable<T> {

    private final OnSubscribeCacheResettable<T> cache;

    public CachedObservable(Observable<T> source) {
        this(new OnSubscribeCacheResettable<T>(source));
    }

    CachedObservable(OnSubscribeCacheResettable<T> cache) {
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