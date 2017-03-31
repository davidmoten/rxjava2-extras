package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Observable;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableReduce<T> extends Flowable<T> {

    private final Flowable<T> source;
    private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
    private final int maxChained;
    private final long maxIterations;
    private final Function<Observable<T>, ? extends Observable<?>> test;

    public FlowableReduce(Flowable<T> source, Function<? super Flowable<T>, ? extends Flowable<T>> reducer,
            int maxChained, int maxIterations, Function<Observable<T>, Observable<?>> test) {
        Preconditions.checkArgument(maxChained > 0, "maxChained must be 1 or greater");
        this.source = source;
        this.reducer = reducer;
        this.maxChained = maxChained;
        this.maxIterations = maxIterations;
        this.test = test;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> child) {

        Flowable<T> f;
        try {
            f = reducer.apply(source);
        } catch (Exception e) {
            Exceptions.throwIfFatal(e);
            child.onSubscribe(SubscriptionHelper.CANCELLED);
            child.onError(e);
            return;
        }
        AtomicReference<Chain<T>> chainRef = new AtomicReference<Chain<T>>();
        FinalReplaySubject<T> destination = new FinalReplaySubject<T>(child, chainRef);
        Chain<T> chain = new Chain<T>(reducer, destination, maxIterations, maxChained, test);
        chainRef.set(chain);
        destination.subscribe(child);
        ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, chain, test);
        chain.initialize(sub);
        f.onTerminateDetach() //
                .subscribe(sub);
    }

    private static enum EventType {
        INITIAL, ADD, DONE, COMPLETE_OR_CANCEL;
    }

    private static final class Event<T> {

        final EventType eventType;
        final ChainedReplaySubject<T> subject;

        Event(EventType eventType, ChainedReplaySubject<T> subject) {
            this.eventType = eventType;
            this.subject = subject;
        }
    }

    @SuppressWarnings("serial")
    private static final class Chain<T> extends AtomicInteger implements Subscription {

        private final Function<? super Flowable<T>, ? extends Flowable<T>> reducer;
        private final SimplePlainQueue<Event<T>> queue;
        private final FinalReplaySubject<T> destination;
        private final long maxIterations;
        private final int maxChained;
        private final Function<Observable<T>, ? extends Observable<?>> test;

        // state
        private int iteration;
        private int length;
        private ChainedReplaySubject<T> finalSubscriber;
        private boolean destinationAttached;
        private volatile boolean cancelled;
        private volatile boolean finished;

        Chain(Function<? super Flowable<T>, ? extends Flowable<T>> reducer, FinalReplaySubject<T> destination,
                long maxIterations, int maxChained, Function<Observable<T>, ? extends Observable<?>> test) {
            this.reducer = reducer;
            this.destination = destination;
            this.maxIterations = maxIterations;
            this.maxChained = maxChained;
            this.test = test;
            this.queue = new SpscLinkedArrayQueue<Event<T>>(16);
        }

        public void initialize(ChainedReplaySubject<T> subject) {
            queue.offer(new Event<T>(EventType.INITIAL, subject));
            drain();
        }

        void testEmitsAdd(ChainedReplaySubject<T> subject) {
            queue.offer(new Event<T>(EventType.ADD, subject));
            drain();
        }

        void testEmitsDone(ChainedReplaySubject<T> subject) {
            subject.finished = true;
            queue.offer(new Event<T>(EventType.DONE, subject));
            drain();
        }

        void completeOrCancel(ChainedReplaySubject<T> subject) {
            queue.offer(new Event<T>(EventType.COMPLETE_OR_CANCEL, subject));
            drain();
        }

        void drain() {
            if (getAndIncrement() == 0) {
                if (cancelled) {
                    if (destinationAttached) {
                        destination.cancel();
                    } else {
                        finalSubscriber.cancel();
                    }
                    queue.clear();
                    return;
                }
                if (destinationAttached) {
                    queue.clear();
                    return;
                }
                int missed = 1;
                while (true) {
                    while (true) {
                        Event<T> v = queue.poll();
                        if (v == null) {
                            break;
                        } else if (destinationAttached) {
                            queue.clear();
                            break;
                        } else if (v.eventType == EventType.INITIAL) {
                            finalSubscriber = v.subject;
                        } else if (v.eventType == EventType.ADD) {
                            if (v.subject == finalSubscriber && length < maxChained - 1) {
                                if (maxIterations == 0 || iteration < maxIterations - 1) {
                                    if (!finalSubscriber.finished) {
                                        // ok to add another subject to the
                                        // chain
                                        ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, this,
                                                test);
                                        addToChain(sub);
                                        finalSubscriber = sub;
                                        iteration++;
                                        length += 1;
                                    }
                                    // otherwise ignore event
                                } else {
                                    // we are at max iterations - 1 so finish up
                                    // by adding the destination (which adds
                                    // another iteration)
                                    addToChain(destination);
                                    destinationAttached = true;
                                    length += 1;
                                    iteration += 1;
                                }
                            }
                            // else ignore event
                        } else if (v.eventType == EventType.DONE && v.subject == finalSubscriber) {
                            addToChain(destination);
                            destinationAttached = true;
                            length++;
                            iteration++;
                        } else {
                            // completeOrCancel
                            if (v.subject == finalSubscriber) {
                                cancelWholeChain();
                            } else if (!finalSubscriber.finished
                                    && (maxIterations == 0 || iteration < maxIterations - 1)) {
                                ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, this, test);
                                addToChain(sub);
                                finalSubscriber = sub;
                                iteration++;
                            } else {
                                length--;
                            }
                            // length unchanged
                        }
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }

        private void addToChain(Subscriber<T> sub) {
            Flowable<T> f;
            try {
                f = reducer.apply(finalSubscriber);
            } catch (Exception e) {
                Exceptions.throwIfFatal(e);
                cancelWholeChain();
                destination.onError(e);
                return;
            }
            f.onTerminateDetach().subscribe(sub);
        }

        private void cancelWholeChain() {
            cancelled = true;
            drain();
        }

        @Override
        public void request(long n) {
            // ignore, just want to be able to cancel
        }

        @Override
        public void cancel() {
            cancelled = true;
            cancelWholeChain();
        }

    }

    private static class FinalReplaySubject<T> extends Flowable<T> implements Subscriber<T>, Subscription {

        private final Subscriber<? super T> child;
        private final AtomicReference<Chain<T>> chain;

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Throwable error;
        private volatile boolean done;
        private volatile boolean cancelled;

        public FinalReplaySubject(Subscriber<? super T> child, AtomicReference<Chain<T>> chain) {
            this.child = child;
            this.chain = chain;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            child.onSubscribe(new MultiSubscription(this, chain.get()));
        }

        @Override
        public void onSubscribe(Subscription parent) {
            long r = requested.get();
            if (r != 0L) {
                parent.request(r);
            }
            drain();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                Subscription p = parent.get();
                if (p != null) {
                    p.request(n);
                }
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this.parent);
        }

        @Override
        public void onNext(T t) {
            queue.offer(t);
            drain();
        }

        @Override
        public void onError(Throwable e) {
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
            // this is a pretty standard drain loop
            // default is to shortcut errors (don't delay them)
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        boolean d = done;
                        Throwable err = error;
                        if (err != null) {
                            queue.clear();
                            error = null;
                            cancel();
                            child.onError(err);
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                cancel();
                                child.onComplete();
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
                        r = requested.addAndGet(-e);
                    }
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

    }

    private static final class Tester<T> extends Observable<T> implements Observer<T> {

        private Observer<? super T> observer;

        @Override
        protected void subscribeActual(Observer<? super T> observer) {
            observer.onSubscribe(Disposables.empty());
            this.observer = observer;
        }

        @Override
        public void onSubscribe(Disposable d) {
            throw new RuntimeException("unexpected");

        }

        @Override
        public void onNext(T t) {
            observer.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            observer.onError(e);
        }

        @Override
        public void onComplete() {
            observer.onComplete();
        }
    }

    private static final class TesterObserver<T> implements Observer<Object> {

        private final Chain<T> chain;
        private final ChainedReplaySubject<T> subject;

        TesterObserver(Chain<T> chain, ChainedReplaySubject<T> subject) {
            this.chain = chain;
            this.subject = subject;
        }

        @Override
        public void onSubscribe(Disposable d) {
            // ignore
        }

        @Override
        public void onNext(Object t) {
            System.out.println("TestObserver for " + subject + " receives " + t);
            chain.testEmitsAdd(subject);
        }

        @Override
        public void onError(Throwable e) {
            subject.cancelWholeChain();
            subject.destination().onError(e);
        }

        @Override
        public void onComplete() {
            chain.testEmitsDone(subject);
        }
    }

    /**
     * Requests minimally of upstream and buffers until this subscriber itself
     * is subscribed to. A maximum of {@code maxDepthConcurrent} subscribers can
     * be chained together at any one time.
     * 
     * @param <T>
     *            generic type
     */
    private static final class ChainedReplaySubject<T> extends Flowable<T>
            implements FlowableSubscriber<T>, Subscription {

        // assigned in constructor
        private final FinalReplaySubject<T> destination;
        private final Chain<T> chain;

        // assigned here
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicLong unreconciledRequests = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscriber<? super T>> child = new AtomicReference<Subscriber<? super T>>();
        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();
        private final Tester<T> tester;

        // mutable
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private boolean childExists;
        private final Function<Observable<T>, ? extends Observable<?>> test;
        volatile boolean finished; // tester finished

        static <T> ChainedReplaySubject<T> create(FinalReplaySubject<T> destination, Chain<T> chain,
                Function<Observable<T>, ? extends Observable<?>> test) {
            ChainedReplaySubject<T> c = new ChainedReplaySubject<T>(destination, chain, test);
            c.init();
            return c;
        }

        private ChainedReplaySubject(FinalReplaySubject<T> destination, Chain<T> chain,
                Function<Observable<T>, ? extends Observable<?>> test) {
            this.destination = destination;
            this.chain = chain;
            this.test = test;
            this.tester = new Tester<T>();
        }

        private void init() {
            Observable<?> o;
            try {
                o = test.apply(tester);
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
            o.subscribe(new TesterObserver<T>(chain, this));
        }

        FinalReplaySubject<T> destination() {
            return destination;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            if (SubscriptionHelper.setOnce(this.parent, parent)) {
                unreconciledRequests.getAndIncrement();
                parent.request(1);
            }
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            System.out.println(this + " subscribed with " + child);
            // only one subscriber expected
            if (!this.child.compareAndSet(null, child)) {
                throw new RuntimeException(this + " cannot subscribe twice");
            }
            child.onSubscribe(this);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                Subscription par = parent.get();
                if (par != null) {
                    if (n < Long.MAX_VALUE) {
                        while (true) {
                            long p = unreconciledRequests.get();
                            long r = Math.max(0, n - p);
                            long p2 = p - (n - r);
                            if (unreconciledRequests.compareAndSet(p, p2)) {
                                if (r > 0) {
                                    par.request(r);
                                }
                                break;
                            }
                        }
                    } else {
                        par.request(Long.MAX_VALUE);
                    }
                }
                drain();
            }
        }

        @Override
        public void onNext(T t) {
            System.out.println(this + " arrived " + t);
            if (done) {
                return;
            }
            queue.offer(t);
            tester.onNext(t);
            if (childExists()) {
                drain();
            } else {
                // make minimal request to keep upstream producing
                unreconciledRequests.incrementAndGet();
                Subscription par = parent.get();
                if (par != null) {
                    par.request(1);
                }
            }
        }

        @Override
        public void onComplete() {
            System.out.println(this + " complete");
            if (done) {
                return;
            }
            done = true;
            cancelParent();
            tester.onComplete();
            if (childExists()) {
                drain();
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            error = t;
            done = true;
            if (childExists()) {
                drain();
            } else {
                cancelWholeChain();
                destination.onError(t);
            }
        }

        private void cancelWholeChain() {
            chain.cancel();
        }

        private boolean childExists() {
            // do a little dance to avoid volatile reads of child
            // TODO establish if worth it via jmh benchmark
            if (childExists) {
                return true;
            } else {
                if (child.get() != null) {
                    childExists = true;
                    return true;
                } else {
                    return false;
                }
            }
        }

        private void drain() {
            // this is a pretty standard drain loop
            // default is to shortcut errors (don't delay them)
            if (wip.getAndIncrement() == 0) {
                int missed = 1;
                while (true) {
                    long r = requested.get();
                    long e = 0;
                    while (e != r) {
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
                        boolean d = done;
                        Throwable err = error;
                        if (err != null) {
                            queue.clear();
                            error = null;
                            cancel();
                            child.get().onError(err);
                            return;
                        }
                        T t = queue.poll();
                        if (t == null) {
                            if (d) {
                                cancel();
                                child.get().onComplete();
                                return;
                            } else {
                                break;
                            }
                        } else {
                            System.out.println(this + " emitting " + t + " to " + child.get());
                            child.get().onNext(t);
                            e++;
                        }
                    }
                    if (e != 0 && r != Long.MAX_VALUE) {
                        r = requested.addAndGet(-e);
                    }
                    missed = wip.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelParentTryToAddSubscriberToChain();
            }
        }

        private void cancelParentTryToAddSubscriberToChain() {
            cancelParent();
            chain.completeOrCancel(this);
        }

        private void cancelParent() {
            Subscription par = parent.get();
            if (par != null) {
                par.cancel();
                // set parent to null so can be GC'd (to avoid
                // GC nepotism use Flowable.onTerminateDetach()
                // upstream)
                parent.set(null);
            }
        }

    }

    private static final class MultiSubscription implements Subscription {

        private final Subscription primary;
        private final Subscription secondary;

        MultiSubscription(Subscription primary, Subscription secondary) {
            this.primary = primary;
            this.secondary = secondary;
        }

        @Override
        public void request(long n) {
            primary.request(n);
        }

        @Override
        public void cancel() {
            primary.cancel();
            secondary.cancel();
        }

    }

}
