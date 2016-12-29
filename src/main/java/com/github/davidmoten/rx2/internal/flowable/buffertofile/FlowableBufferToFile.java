package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.rx2.buffertofile.DataSerializer2;
import com.github.davidmoten.util.ByteArrayOutputStreamNoCopyUnsynchronized;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import rx.exceptions.Exceptions;

public class FlowableBufferToFile<T> extends Flowable<T> {

    private static final byte[] COMPLETED = new byte[] { 92, 41, -99, 85, -103, -76, 11, -21, 43,
            -30, -92, 12, -114, 84, -88, 127 };
    private final Flowable<T> source;
    private final Callable<File> fileFactory;
    private final int pageSize;
    private final DataSerializer2<T> serializer;
    private final Scheduler scheduler;

    public FlowableBufferToFile(Flowable<T> source, Callable<File> fileFactory, int pageSize,
            DataSerializer2<T> serializer, Scheduler scheduler) {
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
        private final DataSerializer2<T> serializer;
        private final Worker worker;
        private final AtomicLong requested = new AtomicLong();

        private Subscription parent;
        private volatile boolean cancelled;
        private volatile boolean done;

        // Is set just before the volatile `done` is set and read just after
        // `done` is read. Thus doesn't need to be volatile.
        private Throwable error;

        BufferToFileSubscriber(Subscriber<? super T> child, MMapQueue queue,
                DataSerializer2<T> serializer, Worker worker) {
            this.child = child;
            this.queue = queue;
            this.serializer = serializer;
            this.worker = worker;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                child.onSubscribe(this);
                parent.request(Long.MAX_VALUE);
            }
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
            parent.cancel();
        }

        @Override
        public void onNext(T t) {
            try {
                ByteArrayOutputStreamNoCopyUnsynchronized bytes = new ByteArrayOutputStreamNoCopyUnsynchronized();
                serializer.serialize(t, bytes);
                // System.out.println("onNext "+
                // Util.getHex(bytes.toByteArray()));
                queue.offer(bytes.toByteArray());
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                onError(e);
                return;
            }
            drain();
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
            int missed = 1;
            while (true) {
                long r = requested.get();
                long e = 0;
                while (e != r) {
                    if (cancelled) {
                        return;
                    }
                    // for visibility purposes must read error after reading
                    // done
                    boolean isDone = done;
                    if (error != null) {
                        // TODO other dispose actions?
                        worker.dispose();
                        cancel();
                        child.onError(error);
                        return;
                    }
                    T t = null;
                    try {
                        byte[] bytes = queue.poll();
                        if (bytes != null) {
                            // System.out.println("read "+ Util.getHex(bytes));
                            System.out.println(Thread.currentThread().getName() + ": polled "
                                    + bytes.length + " bytes");
                            InputStream is = new ByteArrayInputStream(bytes);
                            t = serializer.deserialize(is, bytes.length);
                            // TODO emit error if null?
                        }
                    } catch (Throwable err) {
                        // TODO fatal errors action?
                        Exceptions.throwIfFatal(err);
                        worker.dispose();
                        cancel();
                        child.onError(err);
                        return;
                    }
                    if (t != null) {
                        child.onNext(t);
                        e++;
                    } else if (isDone) {
                        worker.dispose();
                        cancel();
                        child.onComplete();
                        return;
                    } else {
                        break;
                    }
                }
                if (e > 0) {
                    BackpressureHelper.produced(requested, e);
                }
                missed = addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

}
