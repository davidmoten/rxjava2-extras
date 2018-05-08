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

    private static final class DeepTransformSubscriber<T> 
            implements Subscription, Subscriber<T>, Runnable {

        private final BiFunction<Flowable<T>, Runnable, Flowable<T>> transform;
        private final Subscriber<? super T> child;
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicLong requested = new AtomicLong();
        private SimplePlainQueue<T> queue = createQueue();

        
        private Subscription parent;
        private final Flowable<T> source;
        private Throwable error;
        private volatile boolean finished;
        // if can stop transforming
        private volatile boolean doneCalled;
        private volatile boolean cancelled;
        private SimplePlainQueue<T> buffered;
        
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
        }

        @Override
        public void onComplete() {
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

        @Override
        public void cancel() {
            cancelled = true;
        }

        private void drain() {
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
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
        }

    }
    
    private static <T> SimplePlainQueue<T> createQueue() {
        return new SpscLinkedArrayQueue<T>(16);
    }

}
