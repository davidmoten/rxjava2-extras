package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.util.Queue;

import rx.Subscription;

public interface QueueWithSubscription<T> extends Queue<T>, Subscription {

}
