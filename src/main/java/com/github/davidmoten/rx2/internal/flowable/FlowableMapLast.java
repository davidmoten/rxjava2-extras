package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

public final class FlowableMapLast<T> extends Flowable<T> {

	private final Flowable<T> source;
	private final Function<? super T, ? extends T> function;

	public FlowableMapLast(Flowable<T> source, Function<? super T, ? extends T> function) {
		this.source = source;
		this.function = function;
	}

	@Override
	protected void subscribeActual(Subscriber<? super T> s) {
		source.subscribe(new MapLastSubscriber<T>(s, function));
	}

	private final static class MapLastSubscriber<T> implements Subscriber<T>, Subscription {

		private static final Object EMPTY = new Object();

		private final Subscriber<? super T> actual;
		private final Function<? super T, ? extends T> function;
		private final AtomicBoolean firstRequest = new AtomicBoolean(true);

		// mutable state
		@SuppressWarnings("unchecked")
		private T value = (T) EMPTY;
		private Subscription parent;
		private boolean done;

		public MapLastSubscriber(Subscriber<? super T> actual, Function<? super T, ? extends T> function) {
			this.actual = actual;
			this.function = function;
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			if (SubscriptionHelper.validate(this.parent, subscription)) {
				this.parent = subscription;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			if (value == EMPTY) {
				value = t;
			} else {
				actual.onNext(value);
				value = t;
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			if (value != EMPTY) {
				T value2;
				try {
					value2 = function.apply(value);
				} catch (Throwable e) {
					Exceptions.throwIfFatal(e);
					parent.cancel();
					onError(e);
					return;
				}
				actual.onNext(value2);
			}
			done = true;
			actual.onComplete();
		}

		@Override
		public void onError(Throwable e) {
			if (done) {
				return;
			}
			if (value != EMPTY) {
				actual.onNext(value);
			}
			done = true;
			actual.onError(e);
		}

		@Override
		public void cancel() {
			parent.cancel();
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				if (firstRequest.compareAndSet(true, false)) {
					long m = n + 1;
					if (m < 0) {
						m = Long.MAX_VALUE;
					}
					parent.request(m);
				} else {
					parent.request(n);
				}
			}
		}

	}

}
