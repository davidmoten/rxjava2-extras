package com.github.davidmoten.rx2.internal.flowable;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

/**
 * Uses a double-ended queue and collapses entries when they are redundant
 * (whenever a value is added to the queue all values at the end of the queue
 * that are greater or equal to that value are removed).
 * 
 * @param <T>
 *            generic type of stream emissions
 */
public final class FlowableWindowMinMax<T> extends Flowable<T> {

	private final Flowable<T> source;
	private final int windowSize;
	private final Comparator<? super T> comparator;
	private final Metric metric;

	public FlowableWindowMinMax(Flowable<T> source, int windowSize, Comparator<? super T> comparator, Metric metric) {
		Preconditions.checkArgument(windowSize > 0, "windowSize must be greater than zero");
		Preconditions.checkNotNull(comparator, "comparator cannot be null");
		Preconditions.checkNotNull(metric, "metric cannot be null");
		this.source = source;
		this.windowSize = windowSize;
		this.comparator = comparator;
		this.metric = metric;
	}

	@Override
	protected void subscribeActual(org.reactivestreams.Subscriber<? super T> child) {
		source.subscribe(new WindowMinMaxSubscriber<T>(windowSize, comparator, metric, child));
	}

	private static final class WindowMinMaxSubscriber<T> implements Subscriber<T>, Subscription {

		private final int windowSize;
		private final Comparator<? super T> comparator;
		private final Metric metric;
		private final Subscriber<? super T> child;

		// map index to value
		private final Map<Long, T> values;

		// queue of indices
		private final Deque<Long> q;

		private long count = 0;
		private Subscription parent;

		public WindowMinMaxSubscriber(int windowSize, Comparator<? super T> comparator, Metric metric,
		        Subscriber<? super T> child) {
			this.windowSize = windowSize;
			this.comparator = comparator;
			this.metric = metric;
			this.child = child;
			this.values = new HashMap<Long, T>(windowSize);
			this.q = new ArrayDeque<Long>(windowSize);
		}

		@Override
		public void onSubscribe(Subscription parent) {
			if (SubscriptionHelper.validate(this.parent, parent)) {
				this.parent = parent;
				child.onSubscribe(this);
				parent.request(windowSize - 1);
			}
		}

		@Override
		public void request(long n) {
			parent.request(n);
		}

		@Override
		public void cancel() {
			parent.cancel();
			//TODO clear window?
		}

		@Override
		public void onComplete() {
			child.onComplete();
		}

		@Override
		public void onError(Throwable e) {
			child.onError(e);
		}

		@Override
		public void onNext(T t) {
			count++;
			// add to queue
			addToQueue(t);
			if (count >= windowSize) {
				// emit max

				// head of queue is max
				Long head = q.peekFirst();
				final T value;
				if (head == count - windowSize) {
					// if window past that index then remove from map
					values.remove(q.pollFirst());
					value = values.get(q.peekFirst());
				} else {
					value = values.get(head);
				}
				child.onNext(value);
			}
		}

		private void addToQueue(T t) {
			Long v;
			while ((v = q.peekLast()) != null && compare(t, values.get(v)) <= 0) {
				values.remove(q.pollLast());
			}
			values.put(count, t);
			q.offerLast(count);
		}

		private int compare(T a, T b) {
			if (metric == Metric.MIN) {
				return comparator.compare(a, b);
			} else {
				return comparator.compare(b, a);
			}
		}
	}

	public enum Metric {
		MIN, MAX;
	}

}
