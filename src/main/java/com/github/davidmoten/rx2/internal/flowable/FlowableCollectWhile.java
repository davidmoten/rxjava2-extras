package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
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

	private static final class CollectWhileSubscriber<T, R> implements Subscriber<T>, Subscription {

		private final Callable<R> collectionFactory;
		private final BiFunction<? super R, ? super T, ? extends R> add;
		private final BiPredicate<? super R, ? super T> condition;
		private final Subscriber<? super R> child;

		private Subscription parent;
		private R collection;
		private boolean done;
		
		private volatile boolean cancelled;

		private final AtomicInteger state = new AtomicInteger(0);
		//0 == NOT_COMPLETED_NOR_CANCELLED 
		//1 == CANCELLED
		//2 == COMPLETED_AND_CANCELLED
		//3 == COMPLETED_NO_REQUEST

		public CollectWhileSubscriber(Callable<R> collectionFactory, BiFunction<? super R, ? super T, ? extends R> add,
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
			if (collection == null) {
				try {
					collection = collectionFactory.call();
					if (collection == null) {
						throw new NullPointerException("collectionFactory should not return null");
					}
				} catch (Exception e) {
					Exceptions.throwIfFatal(e);
					onError(e);
					return;
				}
			}
			boolean collect;
			try {
				collect = condition.test(collection, t);
			} catch (Throwable e) {
				Exceptions.throwIfFatal(e);
				onError(e);
				return;
			}
			if (collect) {
				try {
					collection = add.apply(collection, t);
					if (collection == null) {
						throw new NullPointerException("add function should not return null");
					}
					parent.request(1);
				} catch (Exception e) {
					Exceptions.throwIfFatal(e);
					onError(e);
					return;
				}
			} else {
				R c = collection;
				collection = null;
				child.onNext(c);
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
			child.onError(e);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			if (collection != null) {
				R c = collection;
				collection = null;
				child.onNext(c);
			}
			if (!cancelled) {
				child.onComplete();
			}
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				parent.request(n);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			parent.cancel();
		}

	}
}
