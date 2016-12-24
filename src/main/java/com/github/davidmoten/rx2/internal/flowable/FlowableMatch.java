package com.github.davidmoten.rx2.internal.flowable;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.internal.fuseable.SimpleQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.internal.operators.BackpressureUtils;

public class FlowableMatch<A, B, K, C> extends Flowable<C> {

    private final Flowable<A> a;
    private final Flowable<B> b;
    private final Func1<? super A, ? extends K> aKey;
    private final Func1<? super B, ? extends K> bKey;
    private final Func2<? super A, ? super B, C> combiner;
    private final long requestSize;

    public FlowableMatch(Flowable<A> a, Flowable<B> b, Func1<? super A, ? extends K> aKey,
            Func1<? super B, ? extends K> bKey, Func2<? super A, ? super B, C> combiner,
            long requestSize) {
        Preconditions.checkNotNull(a, "a should not be null");
        Preconditions.checkNotNull(b, "b should not be null");
        Preconditions.checkNotNull(aKey, "aKey cannot be null");
        Preconditions.checkNotNull(bKey, "bKey cannot be null");
        Preconditions.checkNotNull(combiner, "combiner cannot be null");
        Preconditions.checkArgument(requestSize >= 1, "requestSize must be >=1");
        this.a = a;
        this.b = b;
        this.aKey = aKey;
        this.bKey = bKey;
        this.combiner = combiner;
        this.requestSize = requestSize;
    }

    @Override
    protected void subscribeActual(Subscriber<? super C> child) {
        MatchCoordinator<A, B, K, C> coordinator = new MatchCoordinator<A, B, K, C>(a, b, aKey,
                bKey, combiner, requestSize, child);
        child.onSubscribe(coordinator);
        coordinator.subscribe(child);
    }

    interface Receiver {
        void offer(Object item);
    }

    @SuppressWarnings("serial")
    private static final class MatchCoordinator<A, B, K, C> extends AtomicInteger
            implements Receiver, Subscription {
        private final Map<K, Queue<A>> as = new HashMap<K, Queue<A>>();
        private final Map<K, Queue<B>> bs = new HashMap<K, Queue<B>>();
        private final Flowable<A> a;
        private final Flowable<B> b;
        private final Func1<? super A, ? extends K> aKey;
        private final Func1<? super B, ? extends K> bKey;
        private final Func2<? super A, ? super B, C> combiner;
        private final long requestSize;
        private final SimpleQueue<Object> queue;
        private final Subscriber<? super C> child;
        private final AtomicLong requested = new AtomicLong(0);

        // mutable fields, guarded by `this` atomics
        private int requestFromA = 0;
        private int requestFromB = 0;

        // completion state machine
        private int completed = COMPLETED_NONE;
        // completion states
        private static final int COMPLETED_NONE = 0;
        private static final int COMPLETED_A = 1;
        private static final int COMPLETED_B = 2;
        private static final int COMPLETED_BOTH = 3;

        private MySubscriber<A, K> aSub;
        private MySubscriber<B, K> bSub;

        private volatile boolean cancelled = false;

        MatchCoordinator(Flowable<A> a, Flowable<B> b, Func1<? super A, ? extends K> aKey,
                Func1<? super B, ? extends K> bKey, Func2<? super A, ? super B, C> combiner,
                long requestSize, Subscriber<? super C> child) {
            this.a = a;
            this.b = b;
            this.aKey = aKey;
            this.bKey = bKey;
            this.combiner = combiner;
            this.requestSize = requestSize;
            this.queue = new MpscLinkedQueue<Object>();
            this.child = child;
        }

        public void subscribe(Subscriber<? super C> child) {
            aSub = new MySubscriber<A, K>(Source.A, this, requestSize);
            bSub = new MySubscriber<B, K>(Source.B, this, requestSize);
            a.subscribe(aSub);
            b.subscribe(bSub);
        }

        @Override
        public void request(long n) {
            if (BackpressureUtils.validate(n)) {
                BackpressureUtils.getAndAddRequest(requested, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                cancelAll();
            }
        }

        void cancelAll() {
            aSub.cancel();
            bSub.cancel();
        }

        void drain() {
            if (getAndIncrement() != 0) {
                // work already in progress
                // so exit
                return;
            }
            int missed = 1;
            while (true) {
                long r = requested.get();
                int emitted = 0;
                while (emitted != r) {
                    if (cancelled) {
                        return;
                    }
                    // note will not return null
                    Object v;
                    try {
                        v = queue.poll();
                    } catch (Exception e) {
                        clear();
                        child.onError(e);
                        return;
                    }
                    if (v == null) {
                        // queue is empty
                        break;
                    } else if (v instanceof ItemA) {
                        Emitted em = handleItem(((ItemA) v).value, Source.A);
                        if (em == Emitted.FINISHED) {
                            return;
                        } else if (em == Emitted.ONE) {
                            emitted += 1;
                        }
                    } else if (v instanceof Source) {
                        // source completed
                        Status status = handleCompleted((Source) v);
                        if (status == Status.FINISHED) {
                            return;
                        }
                    } else if (v instanceof MyError) {
                        // v must be an error
                        clear();
                        child.onError(((MyError) v).error);
                        return;
                    } else {
                        // is onNext from B
                        Emitted em = handleItem(v, Source.B);
                        if (em == Emitted.FINISHED) {
                            return;
                        } else if (em == Emitted.ONE) {
                            emitted += 1;
                        }
                    }
                    if (r == emitted) {
                        break;
                    }
                }
                if (emitted > 0) {
                    // reduce requested by emitted
                    BackpressureUtils.produced(requested, emitted);
                }
                missed = this.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }

        private Emitted handleItem(Object value, Source source) {
            final Emitted result;

            // logic duplication occurs below
            // would be nice to simplify without making code
            // unreadable. A bit of a toss-up.
            if (source == Source.A) {
                // look for match
                @SuppressWarnings("unchecked")
                A a = (A) value;
                K key;
                try {
                    key = aKey.call(a);
                } catch (Throwable e) {
                    clear();
                    child.onError(e);
                    return Emitted.FINISHED;
                }
                Queue<B> q = bs.get(key);
                if (q == null) {
                    // cache value
                    add(as, key, a);
                    result = Emitted.NONE;
                } else {
                    // emit match
                    B b = poll(bs, q, key);
                    C c;
                    try {
                        c = combiner.call(a, b);
                    } catch (Throwable e) {
                        clear();
                        child.onError(e);
                        return Emitted.FINISHED;
                    }
                    child.onNext(c);
                    result = Emitted.ONE;
                }
                // if the other source has completed and there
                // is nothing to match with then we should stop
                if (completed == COMPLETED_B && bs.isEmpty()) {
                    // can finish
                    clear();
                    child.onComplete();
                    return Emitted.FINISHED;
                } else {
                    requestFromA += 1;
                }
            } else {
                // look for match
                @SuppressWarnings("unchecked")
                B b = (B) value;
                K key;
                try {
                    key = bKey.call(b);
                } catch (Throwable e) {
                    clear();
                    child.onError(e);
                    return Emitted.FINISHED;
                }
                Queue<A> q = as.get(key);
                if (q == null) {
                    // cache value
                    add(bs, key, b);
                    result = Emitted.NONE;
                } else {
                    // emit match
                    A a = poll(as, q, key);
                    C c;
                    try {
                        c = combiner.call(a, b);
                    } catch (Throwable e) {
                        clear();
                        child.onError(e);
                        return Emitted.FINISHED;
                    }
                    child.onNext(c);
                    result = Emitted.ONE;
                }
                // if the other source has completed and there
                // is nothing to match with then we should stop
                if (completed == COMPLETED_A && as.isEmpty()) {
                    // can finish
                    clear();
                    child.onComplete();
                    return Emitted.FINISHED;
                } else {
                    requestFromB += 1;
                }
            }
            // requests are batched so that each source gets a turn
            checkToRequestMore();
            return result;
        }

        private enum Emitted {
            ONE, NONE, FINISHED;
        }

        private Status handleCompleted(Source source) {
            completed(source);
            final boolean done;
            if (source == Source.A) {
                aSub.cancel();
                done = (completed == COMPLETED_BOTH) || (completed == COMPLETED_A && as.isEmpty());
            } else {
                bSub.cancel();
                done = (completed == COMPLETED_BOTH) || (completed == COMPLETED_B && bs.isEmpty());
            }
            if (done) {
                clear();
                child.onComplete();
                return Status.FINISHED;
            } else {
                checkToRequestMore();
                return Status.KEEP_GOING;
            }
        }

        private enum Status {
            FINISHED, KEEP_GOING;
        }

        private void checkToRequestMore() {
            if (requestFromA == requestSize && completed == COMPLETED_B) {
                requestFromA = 0;
                aSub.request(requestSize);
            } else if (requestFromB == requestSize && completed == COMPLETED_A) {
                requestFromB = 0;
                bSub.request(requestSize);
            } else if (requestFromA == requestSize && requestFromB == requestSize) {
                requestFromA = 0;
                requestFromB = 0;
                aSub.request(requestSize);
                bSub.request(requestSize);
            }
        }

        private void completed(Source source) {
            if (source == Source.A) {
                if (completed == COMPLETED_NONE) {
                    completed = COMPLETED_A;
                } else if (completed == COMPLETED_B) {
                    completed = COMPLETED_BOTH;
                }
            } else {
                if (completed == COMPLETED_NONE) {
                    completed = COMPLETED_B;
                } else if (completed == COMPLETED_A) {
                    completed = COMPLETED_BOTH;
                }
            }
        }

        private void clear() {
            as.clear();
            bs.clear();
            queue.clear();
            aSub.cancel();
            bSub.cancel();
        }

        private static <K, T> void add(Map<K, Queue<T>> map, K key, T value) {
            Queue<T> q = map.get(key);
            if (q == null) {
                q = new LinkedList<T>();
                map.put(key, q);
            }
            q.offer(value);
        }

        private static <K, T> T poll(Map<K, Queue<T>> map, Queue<T> q, K key) {
            T t = q.poll();
            if (q.isEmpty()) {
                map.remove(key);
            }
            return t;
        }

        @Override
        public void offer(Object item) {
            queue.offer(item);
            drain();
        }

    }

    @SuppressWarnings("serial")
    static final class MySubscriber<T, K> extends AtomicReference<Subscription>
            implements Subscription, Subscriber<T> {

        private final Receiver receiver;
        private final Source source;
        private final long requestSize;

        MySubscriber(Source source, Receiver receiver, long requestSize) {
            this.source = source;
            this.receiver = receiver;
            this.requestSize = requestSize;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (SubscriptionHelper.setOnce(this, subscription)) {
                subscription.request(requestSize);
            }
        }

        @Override
        public void request(long n) {
            get().request(n);
        }

        @Override
        public void cancel() {
            SubscriptionHelper.cancel(this);
        }

        @Override
        public void onNext(T t) {
            if (source == Source.A) {
                receiver.offer(new ItemA(t));
            } else {
                receiver.offer(t);
            }
        }

        @Override
        public void onComplete() {
            receiver.offer(source);
        }

        @Override
        public void onError(Throwable e) {
            receiver.offer(new MyError(e));
        }

    }

    static final class MyError {
        final Throwable error;

        MyError(Throwable error) {
            this.error = error;
        }
    }

    static final class ItemA {
        final Object value;

        ItemA(Object value) {
            this.value = value;
        }
    }

    enum Source {
        A, B;
    }

}
