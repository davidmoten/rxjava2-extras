package com.github.davidmoten.rx2.internal.flowable;

import java.io.*;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.*;

import io.reactivex.FlowableSubscriber;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

/**
 * @author David Karnok
 *
 */
public final class FlowableStringInputStream {

    private FlowableStringInputStream() {
        throw new IllegalStateException("No instances!");
    }

    public static InputStream createInputStream(Publisher<String> source, Charset charset) {
        StringInputStream parent = new StringInputStream(charset);
        source.subscribe(parent);
        return parent;
    }

    static final class StringInputStream extends InputStream
    implements FlowableSubscriber<String> {

        final AtomicReference<Subscription> upstream;

        final Charset charset;

        volatile byte[] bytes;

        int index;

        volatile boolean done;
        Throwable error;

        StringInputStream(Charset charset) {
            this.charset = charset;
            upstream = new AtomicReference<Subscription>();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(upstream, s)) {
                s.request(1);
            }
        }

        @Override
        public void onNext(String t) {
            bytes = t.getBytes(charset);
            synchronized (this) {
                notifyAll();
            }
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done = true;
            synchronized (this) {
                notifyAll();
            }
        }

        @Override
        public void onComplete() {
            done = true;
            synchronized (this) {
                notifyAll();
            }
        }

        @Override
        public int read() throws IOException {
            for (;;) {
                byte[] a = awaitBufferIfNecessary();
                if (a == null) {
                    Throwable ex = error;
                    if (ex != null) {
                        if (ex instanceof IOException) {
                            throw (IOException)ex;
                        }
                        throw new IOException(ex);
                    }
                    return -1;
                }
                int idx = index;
                if (idx == a.length) {
                    index = 0;
                    bytes = null;
                    upstream.get().request(1);
                } else {
                    int result = a[idx] & 0xFF;
                    index = idx + 1;
                    return result;
                }
            }
        }

        byte[] awaitBufferIfNecessary() throws IOException {
            byte[] a = bytes;
            if (a == null) {
                synchronized (this) {
                    for (;;) {
                        boolean d = done;
                        a = bytes;
                        if (a != null) {
                            break;
                        }
                        if (d || upstream.get() == SubscriptionHelper.CANCELLED) {
                            break;
                        }
                        try {
                            wait();
                        } catch (InterruptedException ex) {
                            if (upstream.get() != SubscriptionHelper.CANCELLED) {
                                InterruptedIOException exc = new InterruptedIOException();
                                exc.initCause(ex);
                                throw exc;
                            }
                            break;
                        }
                    } 
                }
            }
            return a;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (off < 0 || len < 0 || off >= b.length || off + len > b.length) {
                throw new IndexOutOfBoundsException("b.length=" + b.length + ", off=" + off + ", len=" + len);
            }
            for (;;) {
                byte[] a = awaitBufferIfNecessary();
                if (a == null) {
                    Throwable ex = error;
                    if (ex != null) {
                        if (ex instanceof IOException) {
                            throw (IOException)ex;
                        }
                        throw new IOException(ex);
                    }
                    return -1;
                }
                int idx = index;
                if (idx == a.length) {
                    index = 0;
                    bytes = null;
                    upstream.get().request(1);
                } else {
                    int r = 0;
                    while (idx < a.length && len > 0) {
                        b[off] = a[idx];
                        idx++;
                        off++;
                        r++;
                        len--;
                    }
                    index = idx;
                    return r;
                }
            }
        }

        @Override
        public int available() throws IOException {
            byte[] a = bytes;
            int idx = index;
            return a != null ? Math.max(0, a.length - idx) : 0;
        }

        @Override
        public void close() throws IOException {
            SubscriptionHelper.cancel(upstream);
            synchronized (this) {
                notifyAll();
            }
        }
    }
}