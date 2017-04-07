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
            int maxChained, long maxIterations, Function<Observable<T>, Observable<?>> test) {
        Preconditions.checkArgument(maxChained > 0, "maxChained must be 1 or greater");
        Preconditions.checkArgument(maxIterations > 0, "maxIterations must be 1 or greater");
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
        DestinationSerializedSubject<T> destination = new DestinationSerializedSubject<T>(child, chainRef);
        Chain<T> chain = new Chain<T>(reducer, destination, maxIterations, maxChained, test);
        chainRef.set(chain);
        destination.subscribe(child);
        ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, chain, test);
        chain.initialize(sub);
        f.onTerminateDetach() //
                .subscribe(sub);
    }

    private static enum EventType {
        ADD, DONE, COMPLETE_OR_CANCEL;
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
        private final DestinationSerializedSubject<T> destination;
        private final long maxIterations;
        private final int maxChained;
        private final Function<Observable<T>, ? extends Observable<?>> test;

        // state
        private int iteration = 1;
        private int length;
        private ChainedReplaySubject<T> finalSubscriber;
        private boolean destinationAttached;
        private volatile boolean cancelled;

        Chain(Function<? super Flowable<T>, ? extends Flowable<T>> reducer, DestinationSerializedSubject<T> destination,
                long maxIterations, int maxChained, Function<Observable<T>, ? extends Observable<?>> test) {
            this.reducer = reducer;
            this.destination = destination;
            this.maxIterations = maxIterations;
            this.maxChained = maxChained;
            this.test = test;
            this.queue = new SpscLinkedArrayQueue<Event<T>>(16);
        }

        public void initialize(ChainedReplaySubject<T> subject) {
            finalSubscriber = subject;
            if (maxIterations == 1) {
                finalSubscriber.subscribe(destination);
                destinationAttached = true;
            }
        }

        void tryAddSubscriber(ChainedReplaySubject<T> subject) {
            queue.offer(new Event<T>(EventType.ADD, subject));
            drain();
        }

        void done(ChainedReplaySubject<T> subject) {
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
                        finalSubscriber.cancel();
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
                            debug("clearing queue");
                            queue.clear();
                            break;
                        } else if (v.eventType == EventType.ADD) {
                            handleAdd(v);
                        } else if (v.eventType == EventType.DONE) {
                            handleDone();
                        } else {
                            handleCompleteOrCancel(v);
                        }
                    }
                    missed = addAndGet(-missed);
                    if (missed == 0) {
                        break;
                    }
                }
            }
        }

        private void handleAdd(Event<T> v) {
            debug("ADD " + v.subject);
            if (v.subject == finalSubscriber && length < maxChained) {
                if (iteration <= maxIterations - 1) {
                    // ok to add another subject to the
                    // chain
                    ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, this, test);
                    if (iteration == maxIterations - 1) {
                        sub.subscribe(destination);
                        debug(sub + "subscribed to by destination");
                        destinationAttached = true;
                    }
                    addToChain(sub);
                    finalSubscriber = sub;
                    iteration++;
                    length += 1;
                }
            }
        }

        private void handleDone() {
            debug("DONE");
            destinationAttached = true;
            finalSubscriber.subscribe(destination);
        }

        private void handleCompleteOrCancel(Event<T> v) {
            debug("COMPLETE/CANCEL " + v.subject);
            if (v.subject == finalSubscriber) {
                // TODO what to do here?
                // cancelWholeChain();
            } else if (iteration < maxIterations - 1) {
                ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, this, test);
                addToChain(sub);
                finalSubscriber = sub;
                iteration++;
            } else if (iteration == maxIterations - 1) {
                ChainedReplaySubject<T> sub = ChainedReplaySubject.create(destination, this, test);
                destinationAttached = true;
                sub.subscribe(destination);
                addToChain(sub);
                debug(sub + "subscribed to by destination");
                finalSubscriber = sub;
                iteration++;
            } else {
                length--;
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
            debug(finalSubscriber + " subscribed to by " + sub);
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

    private static class DestinationSerializedSubject<T> extends Flowable<T>
            implements FlowableSubscriber<T>, Subscription {

        private final Subscriber<? super T> child;
        private final AtomicReference<Chain<T>> chain;

        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<Subscription> parent = new AtomicReference<Subscription>();
        private final AtomicLong requested = new AtomicLong();
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);

        private Throwable error;
        private volatile boolean done;
        private volatile boolean cancelled;
        private final AtomicLong deferredRequests = new AtomicLong();

        DestinationSerializedSubject(Subscriber<? super T> child, AtomicReference<Chain<T>> chain) {
            this.child = child;
            this.chain = chain;
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            debug(this + " subscribed to by " + child);
            child.onSubscribe(new MultiSubscription(this, chain.get()));
            // don't need to drain because destination is always subscribed to
            // this before this is subscribed to parent
        }

        @Override
        public void onSubscribe(Subscription pr) {
            parent.set(pr);
            long r = deferredRequests.getAndSet(-1);
            if (r > 0L) {
                debug(this + " requesting of parent " + r);
                pr.request(r);
            }
            drain();
        }

        @Override
        public void request(long n) {
            debug(this + " request " + n);
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                while (true) {
                    Subscription p = parent.get();
                    long d = deferredRequests.get();
                    if (d == -1) {
                        debug(this + " requesting from parent " + n);
                        p.request(n);
                    } else {
                        long d2 = d + n;
                        if (d2 < 0) {
                            d2 = Long.MAX_VALUE;
                        }
                        if (deferredRequests.compareAndSet(d, d2)) {
                            break;
                        }
                    }
                }
                drain();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            SubscriptionHelper.cancel(this.parent);
            chain.get().cancel();
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
            debug("final complete");
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
            debug(subject + " TestObserver emits add " + t);
            chain.tryAddSubscriber(subject);
        }

        @Override
        public void onError(Throwable e) {
            subject.cancelWholeChain();
            subject.destination().onError(e);
        }

        @Override
        public void onComplete() {
            debug(subject + " TestObserver emits done");
            chain.done(subject);
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
        private final DestinationSerializedSubject<T> destination;
        private final Chain<T> chain;

        // assigned here
        private final SimplePlainQueue<T> queue = new SpscLinkedArrayQueue<T>(16);
        private final AtomicLong requested = new AtomicLong();
        private final AtomicReference<Requests<T>> requests = new AtomicReference<Requests<T>>(
                new Requests<T>(null, 0, 0, null));
        private final AtomicInteger wip = new AtomicInteger();
        private final Tester<T> tester;

        // mutable
        private volatile boolean done;
        private Throwable error;
        private volatile boolean cancelled;
        private final Function<Observable<T>, ? extends Observable<?>> test;

        static <T> ChainedReplaySubject<T> create(DestinationSerializedSubject<T> destination, Chain<T> chain,
                Function<Observable<T>, ? extends Observable<?>> test) {
            ChainedReplaySubject<T> c = new ChainedReplaySubject<T>(destination, chain, test);
            c.init();
            return c;
        }

        private ChainedReplaySubject(DestinationSerializedSubject<T> destination, Chain<T> chain,
                Function<Observable<T>, ? extends Observable<?>> test) {
            this.destination = destination;
            this.chain = chain;
            this.test = test;
            this.tester = new Tester<T>();
        }

        private static final class Requests<T> {
            final Subscription parent;
            final long unreconciled;
            final long deferred;
            final Subscriber<? super T> child;

            Requests(Subscription parent, long unreconciled, long deferred, Subscriber<? super T> child) {
                this.parent = parent;
                this.unreconciled = unreconciled;
                this.deferred = deferred;
                this.child = child;
            }
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

        DestinationSerializedSubject<T> destination() {
            return destination;
        }

        @Override
        public void onSubscribe(Subscription parent) {
            while (true) {
                Requests<T> r = requests.get();
                Requests<T> r2;
                if (r.deferred == 0) {
                    r2 = new Requests<T>(parent, r.unreconciled + 1, 0, r.child);
                    if (requests.compareAndSet(r, r2)) {
                        parent.request(1);
                        break;
                    }
                } else {
                    r2 = new Requests<T>(parent, r.unreconciled, 0, r.child);
                    if (requests.compareAndSet(r, r2)) {
                        parent.request(r.deferred);
                        break;
                    }
                }
            }
            drain();
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> child) {
            debug(this + " subscribed with " + child);
            // only one subscriber expected
            while (true) {
                Requests<T> r = requests.get();
                if (r.child != null) {
                    throw new RuntimeException("unexpected");
                }
                Requests<T> r2 = new Requests<T>(r.parent, r.unreconciled, r.deferred, child);
                if (requests.compareAndSet(r, r2)) {
                    break;
                }
            }
            child.onSubscribe(this);
            drain();
        }

        @Override
        public void request(long n) {
            debug(this + " request " + n);
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.add(requested, n);
                while (true) {
                    Requests<T> r = requests.get();
                    Requests<T> r2;
                    if (r.parent == null) {
                        long d = r.deferred + n;
                        if (d < 0) {
                            d = Long.MAX_VALUE;
                        }
                        r2 = new Requests<T>(r.parent, r.unreconciled, d, r.child);
                        if (requests.compareAndSet(r, r2)) {
                            break;
                        }
                    } else {
                        long x = n + r.deferred - r.unreconciled;
                        long u = Math.max(0, -x);
                        r2 = new Requests<T>(r.parent, u, 0, r.child);
                        if (requests.compareAndSet(r, r2)) {
                            if (x > 0) {
                                r.parent.request(x);
                            }
                            break;
                        }
                    }
                }
                drain();
            }
        }

        @Override
        public void onNext(T t) {
            debug(this + " arrived " + t);
            if (done) {
                return;
            }
            queue.offer(t);
            tester.onNext(t);
            while (true) {
                Requests<T> r = requests.get();
                Requests<T> r2;
                if (r.child == null) {
                    r2 = new Requests<T>(r.parent, r.unreconciled + 1, r.deferred, r.child);
                    if (requests.compareAndSet(r, r2)) {
                        // make minimal request to keep upstream producing
                        r.parent.request(1);
                        break;
                    }
                } else {
                    r2 = new Requests<T>(r.parent, r.unreconciled, 0, r.child);
                    if (requests.compareAndSet(r, r2)) {
                        break;
                    }
                }
            }
            drain();
        }

        @Override
        public void onComplete() {
            debug(this + " complete");
            if (done) {
                return;
            }
            done = true;
            cancelParent();
            debug(this + " emits complete to tester");
            tester.onComplete();
            drain();
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            error = t;
            done = true;
            drain();
        }

        private void cancelWholeChain() {
            chain.cancel();
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
                        Subscriber<? super T> child;
                        while (true) {
                            Requests<T> req = requests.get();
                            Requests<T> req2 = new Requests<T>(req.parent, req.unreconciled, req.deferred, req.child);
                            if (requests.compareAndSet(req, req2)) {
                                child = req.child;
                                break;
                            }
                        }
                        boolean d = done;
                        Throwable err = error;
                        if (child == null) {
                            if (err != null) {
                                queue.clear();
                                error = null;
                                cancel();
                                destination.onError(err);
                            }
                            break;
                        }
                        if (cancelled) {
                            queue.clear();
                            return;
                        }
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
                            debug(this + " emitting " + t + " to " + requests.get().child + ":"
                                    + requests.get().child.getClass().getSimpleName());
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
            Subscription par = requests.get().parent;
            if (par != null) {
                par.cancel();
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

    static void debug(String message) {
        // System.out.println(message);
    }

}
