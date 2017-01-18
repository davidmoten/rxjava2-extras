package com.github.davidmoten.rx2.internal.flowable;

import org.reactivestreams.Subscriber;

import com.github.davidmoten.rx2.buffertofile.Serializer;
import com.github.davidmoten.rx2.persist.Options;

import io.reactivex.Flowable;

public class FlowablePersisting<T> extends Flowable<T> {

    private final Options options;
    private final Serializer<T> serializer;

    public FlowablePersisting(Options options, Serializer<T> serializer) {
        this.options = options;
        this.serializer = serializer;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        
    }

}
