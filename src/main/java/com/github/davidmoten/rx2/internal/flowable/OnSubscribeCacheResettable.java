package com.github.davidmoten.rx2.internal.flowable;

import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

public final class OnSubscribeCacheResettable<T>{

    private final AtomicBoolean refresh = new AtomicBoolean(true);
    private final Flowable<T> source;
    private volatile Flowable<T> current;

    public OnSubscribeCacheResettable(Flowable<T> source) {
        this.source = source;
        this.current = source;
    }

    public void subscribe(final Subscriber<? super T> subscriber) {
        if (refresh.compareAndSet(true, false)) {
            current = source.cache();
        }
        current.subscribe(subscriber);
    }

    public void reset() {
        refresh.set(true);
    }

}
