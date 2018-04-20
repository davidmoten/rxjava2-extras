package com.github.davidmoten.rx2.internal.flowable;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public class FlowablePipeOut extends Flowable<byte[]> {

    private final Flowable<byte[]> source;
    private final Function<OutputStream, OutputStream> transform;
    private final int bufferSize;

    public FlowablePipeOut(Flowable<byte[]> source, Function<OutputStream, OutputStream> transform, int bufferSize) {
        this.source = source;
        this.transform = transform;
        this.bufferSize = bufferSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super byte[]> s) {
        PipeOutSubscriber subscriber;
        try {
            subscriber = new PipeOutSubscriber(source, transform, bufferSize, s);
        } catch (Exception e) {
            s.onError(e);
            return;
        }
        source.subscribe(subscriber);
        s.onSubscribe(subscriber);
    }

    private static final class PipeOutSubscriber extends OutputStream implements Subscriber<byte[]>, Subscription {

        private final Flowable<byte[]> source;
        private final Function<OutputStream, OutputStream> transform;
        private Subscription s;
        private SimplePlainQueue<ByteBuffer> queue = new SpscLinkedArrayQueue<ByteBuffer>(16);
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicLong requested = new AtomicLong();
        private long emitted;
        private final OutputStream out;
        private final Subscriber<? super byte[]> child;
        private boolean done;
        private final int batchSize = 16;
        private int count;
        private volatile boolean cancelled;

        public PipeOutSubscriber(Flowable<byte[]> source, Function<OutputStream, OutputStream> transform,
                int bufferSize, Subscriber<? super byte[]> child) throws Exception {
            this.source = source;
            this.transform = transform;
            this.child = child;
            if (bufferSize == 0) {
                this.out = transform.apply(this);
            } else {
                this.out = transform.apply(new BufferedOutputStream(this, bufferSize));
            }
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.s = s;
            s.request(batchSize);
        }

        @Override
        public void onNext(byte[] b) {
            count++;
            if (done) {
                return;
            }
            try {
                out.write(b);
                drain();
            } catch (IOException e) {
                onError(e);
            }
        }

        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = emitted;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        
                        
                    }
                    emitted = e;
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            done = true;
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
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
            // TODO Auto-generated method stub

        }

        @Override
        public void write(int b) throws IOException {
            queue.offer(ByteBuffer.allocate(1));
            drain();
        }

        @Override
        public void write(byte[] b) throws IOException {
            queue.offer(ByteBuffer.wrap(b));
            drain();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            queue.offer(ByteBuffer.wrap(b, off, len));
            drain();
        }

    }
}
