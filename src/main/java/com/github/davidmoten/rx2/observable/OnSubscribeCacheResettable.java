package com.github.davidmoten.rx2.observable;

import io.reactivex.Observable;
import io.reactivex.Observer;

import java.util.concurrent.atomic.AtomicBoolean;

public final class OnSubscribeCacheResettable<T>{

    private final AtomicBoolean refresh = new AtomicBoolean(true);
    private final Observable<T> source;
    private volatile Observable<T> current;

    public OnSubscribeCacheResettable(Observable<T> source) {
        this.source = source;
        this.current = source;
    }

    public void subscribe(final Observer<? super T> observer) {
        if (refresh.compareAndSet(true, false)) {
            current = source.cache();
        }
        current.subscribe(observer);
    }

    public void reset() {
        refresh.set(true);
    }

}
