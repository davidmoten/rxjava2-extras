package com.github.davidmoten.rx2;

import io.reactivex.functions.Predicate;

public final class Predicates {

	private Predicates() {
		// prevent instantiation
	}

	@SuppressWarnings("unchecked")
	public static <T> Predicate<T> alwaysFalse() {
		return (Predicate<T>) FalseHolder.INSTANCE;
	}

	private static final class FalseHolder {
		static final Predicate<Object> INSTANCE = new Predicate<Object>() {
			@Override
			public boolean test(Object t) throws Exception {
				return false;
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static <T> Predicate<T> alwaysTrue() {
		return (Predicate<T>) TrueHolder.INSTANCE;
	}

	private static final class TrueHolder {
		static final Predicate<Object> INSTANCE = new Predicate<Object>() {
			@Override
			public boolean test(Object t) throws Exception {
				return true;
			}
		};
	}
	
}
