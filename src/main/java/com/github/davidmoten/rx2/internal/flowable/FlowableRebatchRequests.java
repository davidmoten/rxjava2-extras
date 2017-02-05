package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.functions.LongConsumer;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableRebatchRequests<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Flowable<Long> requests;

    public FlowableRebatchRequests(Flowable<T> source, Flowable<Long> requests) {
        this.source = source;
        this.requests = requests;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {
        source.subscribe(new RebatchSubscriber<T>(child, requests));
    }

    @SuppressWarnings("serial")
    private static final class RebatchSubscriber<T> extends AtomicInteger implements Subscriber<T>, Subscription {

        private final Subscriber<? super T> child;
        private final Flowable<Long> requests;
        private final AtomicLong requested;
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Subscription parent;
        private RequestSubscriber requestSubscriber;
        private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
        private volatile boolean done;
        private volatile long nextRequest;
        private volatile boolean requestsArrived = true;
        private long count;

        RebatchSubscriber(Subscriber<? super T> child, Flowable<Long> requests) {
            this.child = child;
            this.requests = requests;
            this.requested = new AtomicLong();
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.validate(this.parent, parent)) {
                this.parent = parent;
                this.requestSubscriber = new RequestSubscriber(this);
                requests.subscribe(requestSubscriber);
                requestSubscriber.request(1);
                child.onSubscribe(this);
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
            parent.cancel();
            requestSubscriber.cancel();
        }

        @Override
        public void onNext(T t) {
            long c = count;
            if (--c == 0) {
                requestsArrived = true;
                count = nextRequest;
            } else {
                count = c;
            }
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable e) {
            if (error.compareAndSet(null, e)) {
                done = true;
            } else {
                RxJavaPlugins.onError(e);
            }
        }

        @Override
        public void onComplete() {
            done = true;
        }

        void drain() {
            if (getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    int e = 0;
                    boolean reqsArrived = requestsArrived;
                    while (e != r) {
                        boolean d = done;
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                Throwable err = error.get();
                                if (err != null) {
                                    parent.cancel();
                                    child.onError(err);
                                } else {
                                    parent.cancel();
                                    child.onComplete();
                                }
                                return;
                            } else {
                                break;
                            }
                        } else {
                            child.onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        requested.addAndGet(-e);
                    }
                    if (e != r && reqsArrived) {
                        requestsArrived = false;
                        long nr = nextRequest;
                        if (nr > 0) {
                            parent.request(nr);
                            requestSubscriber.request(1);
                        }
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        public void nextRequest(Long n) {
            nextRequest = n;
            drain();
        }

        public void requestErrored(Throwable e) {
            if (done) {
                RxJavaPlugins.onError(e);
                return;
            }
            if (error.compareAndSet(null, e)) {
                done = true;
            } else {
                RxJavaPlugins.onError(e);
            }
            drain();
        }

    }

    private static final class RequestSubscriber implements Subscriber<Long>, Subscription {

        private final RebatchSubscriber<?> rebatchSubscriber;
        private Subscription parent;

        public RequestSubscriber(RebatchSubscriber<?> rebatchSubscriber) {
            this.rebatchSubscriber = rebatchSubscriber;
        }

        @Override
        public void request(long n) {
            parent.request(n);
        }

        @Override
        public void cancel() {
            parent.cancel();
        }

        @Override
        public void onSubscribe(Subscription parent) {
            this.parent = parent;
        }

        @Override
        public void onNext(Long n) {
            rebatchSubscriber.nextRequest(n);
        }

        @Override
        public void onError(Throwable e) {
            rebatchSubscriber.requestErrored(e);
        }

        @Override
        public void onComplete() {
            // do nothing
        }

    }

    public static void main(String[] args) throws InterruptedException {
        Flowable<Integer> o = Flowable.range(1, 10).doOnRequest(new LongConsumer() {

            @Override
            public void accept(long t) throws Exception {
                System.out.println(t);
            }
        });
        new FlowableRebatchRequests<Integer>(o, Flowable.just(1L, 2L, 3L, 4L)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete()

        ;
    }

}
