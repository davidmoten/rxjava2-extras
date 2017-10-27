package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.flowables.ConnectableFlowable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

public class FlowableRefCountNonBlocking<T> extends Flowable<T> {

    private final ConnectableFlowable<T> source;
    private final SimplePlainQueue<Object> queue;
    private final AtomicInteger wip = new AtomicInteger();

    public FlowableRefCountNonBlocking(ConnectableFlowable<T> source) {
        this.source = source;
        this.queue = new MpscLinkedQueue<Object>();
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        queue.offer(child);
        drain();
    }
    
    private void drain() {
        if (wip.getAndIncrement() == 0) {
            while (true) {
                Object o;
                while ((o = queue.poll()) != null) {
                    // TODO do something with o
                    if (wip.decrementAndGet() == 0) {
                        return;
                    }
                }
            }
        }
    }
    
}
