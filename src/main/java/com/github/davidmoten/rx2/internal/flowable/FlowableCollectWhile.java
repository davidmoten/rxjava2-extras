package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class FlowableCollectWhile<T, R> extends Flowable<R> {

	private final Flowable<T> source;
	private final Callable<R> collectionFactory;
	private final BiFunction<? super R, ? super T, ? extends R> add;
	private final BiPredicate<? super R, ? super T> condition;

	public FlowableCollectWhile(Flowable<T> source, Callable<R> collectionFactory,
	        BiFunction<? super R, ? super T, ? extends R> add, BiPredicate<? super R, ? super T> condition) {
		super();
		this.source = source;
		this.collectionFactory = collectionFactory;
		this.add = add;
		this.condition = condition;
	}

	@Override
	protected void subscribeActual(Subscriber<? super R> child) {
		CollectWhileSubscriber<T, R> subscriber = new CollectWhileSubscriber<T, R>(collectionFactory, add, condition,
		        child);
		source.subscribe(subscriber);
	}

	private static final class CollectWhileSubscriber<T, R> extends AtomicInteger
	        implements Subscriber<T>, Subscription {

		private final Callable<R> collectionFactory;
		private final BiFunction<? super R, ? super T, ? extends R> add;
		private final BiPredicate<? super R, ? super T> condition;
		private final Subscriber<? super R> child;
		private final AtomicLong requested = new AtomicLong();

		private Subscription parent;
		private R collection;
		private R collectionToEmit;
		private boolean done;
		private Throwable error;
		private volatile boolean terminalEventReceived;

		private volatile boolean cancelled;

		CollectWhileSubscriber(Callable<R> collectionFactory, BiFunction<? super R, ? super T, ? extends R> add,
		        BiPredicate<? super R, ? super T> condition, Subscriber<? super R> child) {
			this.collectionFactory = collectionFactory;
			this.add = add;
			this.condition = condition;
			this.child = child;
		}

		@Override
		public void onSubscribe(Subscription parent) {
			this.parent = parent;
			child.onSubscribe(this);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			if (collection == null && !collectionCreated()) {
				return;
			}
			boolean collect;
			try {
				collect = condition.test(collection, t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}
			if (!collect) {
				collectionToEmit = collection;
				if (!collectionCreated()) {
					return;
				}
			}
			try {
				collection = add.apply(collection, t);
				if (collection == null) {
					throw new NullPointerException("add function should not return null");
				}
			} catch (Exception e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}
			drain();
		}

		public boolean collectionCreated() {
			try {
				collection = collectionFactory.call();
				if (collection == null) {
					throw new NullPointerException("collectionFactory should not return null");
				}
				return true;
			} catch (Exception e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return false;
			}
		}

		@Override
		public void onError(Throwable e) {
			if (done) {
				RxJavaPlugins.onError(e);
				return;
			}
			done = true;
			collection = null;
			parent.cancel();
			error = e;
			terminalEventReceived = true;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			collectionToEmit = collection;
			collection = null;
			terminalEventReceived = true;
			drain();
		}

		private void drain() {
			if (getAndIncrement() == 0) {
				int missed = 1;
				while (true) {
					long r = requested.get();
					boolean emitted = false;
					boolean terminal = terminalEventReceived;
					if (terminal && error != null) {
						parent.cancel();
						collection = null;
						collectionToEmit = null;
						child.onError(error);
						return;
					}
					if (r > 0) {
						R c = collectionToEmit;
						if (c != null) {
							collectionToEmit = null;
							child.onNext(c);
							emitted = true;
						}
					}
					if (emitted) {
						BackpressureHelper.add(requested, -1);
					}
					missed = addAndGet(-missed);
					if (missed == 0) {
						return;
					}
				}
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

	}
}
