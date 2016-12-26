package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.rx2.buffertofile.DataSerializer;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableBufferToFile<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Callable<File> fileFactory;
    private final int pageSize;
    private final DataSerializer<T> serializer;
    private final Scheduler scheduler;

    public FlowableBufferToFile(Flowable<T> source, Callable<File> fileFactory, int pageSize,
            DataSerializer<T> serializer, Scheduler scheduler) {
        this.source = source;
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
        this.serializer = serializer;
        this.scheduler = scheduler;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new BufferToFileSubscriber<T>(child, new MMapQueue(fileFactory, pageSize),
                serializer, scheduler.createWorker()));
    }

    @SuppressWarnings({ "unused", "serial" })
    private static final class BufferToFileSubscriber<T> extends AtomicInteger
            implements Subscriber<T>, Subscription, Runnable {

        private final Subscriber<? super T> child;
        private final MMapQueue queue;
        private final DataSerializer<T> serializer;
        private final Worker worker;
        private final AtomicLong requested = new AtomicLong();

        private Subscription parent;
        private volatile boolean cancelled;
        private volatile boolean done;

        // Is set just before the volatile `done` is set and read just after
        // `done` is read. Thus doesn't need to be volatile.
        private Throwable error;

        BufferToFileSubscriber(Subscriber<? super T> child, MMapQueue queue,
                DataSerializer<T> serializer, Worker worker) {
            this.child = child;
            this.queue = queue;
            this.serializer = serializer;
            this.worker = worker;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void onNext(T t) {
        }

        @Override
        public void onError(Throwable e) {
            // must assign error before assign done = true to avoid race
            // condition in finished() and also so appropriate memory barrier in
            // place given error is non-volatile
            error = e;
            done = true;
            drain();
        }

        @Override
        public void onComplete() {
            done = true;
            drain();
        }

        private void drain() {
            // only schedule a drain if current drain has finished
            // otherwise the drainRequested counter will be incremented
            // and the drain loop will ensure that another drain cycle occurs if
            // required
            if (!cancelled && getAndIncrement() == 0) {
                worker.schedule(this);
            }
        }

        // this method executed from drain() only
        @Override
        public void run() {
            // catch exceptions related to file based queue in drainNow()
            try {
                drainNow();
            } catch (Throwable e) {
                child.onError(e);
            }
        }

        private void drainNow() {

        }
    }

}
