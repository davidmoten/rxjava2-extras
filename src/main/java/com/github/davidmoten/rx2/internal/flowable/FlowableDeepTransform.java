package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

public class FlowableDeepTransform<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final BiFunction<Flowable<T>, Runnable, Flowable<T>> transform;

    public FlowableDeepTransform(Flowable<T> source, BiFunction<Flowable<T>, Runnable, Flowable<T>> transform) {
        this.source = source;
        this.transform = transform;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        DeepTransformSubscriber<T> subscription = new DeepTransformSubscriber<T>(source, transform, s);
        s.onSubscribe(subscription);
        source.subscribe(subscription);
    }

    private static final class DeepTransformSubscriber<T> implements Subscription, Subscriber<T>, Runnable {

        private final BiFunction<Flowable<T>, Runnable, Flowable<T>> transform;
        final Subscriber<? super T> child;
        
        //wip stays at 1 until doneCalled
        private final AtomicInteger wip = new AtomicInteger(1);
        private final AtomicLong requested = new AtomicLong();
        SimplePlainQueue<T> queue = createQueue();
        SimplePlainQueue<T> queue2 = createQueue();
        private Subscription parent;
        private final Flowable<T> source;
        private Throwable error;
        private volatile boolean finished;
        // if can stop transforming
        private volatile boolean doneCalled;
        private volatile boolean cancelled;
        private SimplePlainQueue<T> buffered;
        private long emitted;

        public DeepTransformSubscriber(Flowable<T> source, BiFunction<Flowable<T>, Runnable, Flowable<T>> transform,
                Subscriber<? super T> child) {
            this.source = source;
            this.transform = transform;
            this.child = child;
        }

        @Override
        public void onSubscribe(Subscription s) {
            Flowable<T> f;
            try {
                f = transform.apply(source, this);
            } catch (Throwable e) {
                Exceptions.throwIfFatal(e);
                this.parent = SubscriptionHelper.CANCELLED;
                s.cancel();
                child.onError(e);
                return;
            }
            this.parent = s;
            f.subscribe(this);
            child.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
            }
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            finished = true;
            drain();
        }

        @Override
        public void onComplete() {
            finished = true;
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = emitted;
                    boolean d = doneCalled;
                    
                    break;
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            // complete so report what's buffered and pass through future emissions
            doneCalled = true;
            wip.set(0);
            drain();
        }

        public void transformCompleted() {
            if (doneCalled) {
                // emit all
                while (true) {
                    T t = queue.poll();
                    if (t != null) {
                        child.onNext(t);
                    } else {
                        child.onComplete();
                        break;
                    }
                }
            } else {
                
            }
        }

        public void transformErrored(Throwable t) {
            // TODO Auto-generated method stub
            
        }

    }

    private static final class FlowableBetweenQueues<T> extends Flowable<T> implements Subscriber<T>, Subscription {

        private final DeepTransformSubscriber<T> dt;
        private final AtomicLong requested = new AtomicLong();
        private long emitted;
        private final AtomicInteger wip = new AtomicInteger();
        private volatile boolean cancelled;

        private Subscription transformed;
        private Subscriber<? super T> subscriber;

        FlowableBetweenQueues(DeepTransformSubscriber<T> dt) {
            super();
            this.dt = dt;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
            subscriber.onSubscribe(this);
        }

        @Override
        public void onSubscribe(Subscription s) {
            transformed = s;
            transformed.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T t) {
            dt.queue2.offer(t);
        }

        @Override
        public void onError(Throwable t) {
            dt.transformErrored(t);
        }

        @Override
        public void onComplete() {
            dt.transformCompleted();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                drain();
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
                            dt.queue.clear();
                            return;
                        }
                        T t = dt.queue.poll();
                        if (t == null) {
                            subscriber.onComplete();
                            return;
                        } else {
                            subscriber.onNext(t);
                            e++;
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
        public void cancel() {
            cancelled = true;
        }

    }

    private static <T> SimplePlainQueue<T> createQueue() {
        return new SpscLinkedArrayQueue<T>(16);
    }

}
