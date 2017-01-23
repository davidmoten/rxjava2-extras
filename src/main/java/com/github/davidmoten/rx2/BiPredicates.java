package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.functions.BiPredicate;

public final class BiPredicates {

	private BiPredicates() {
		// prevent instantiation
	}

	public static <T, R> BiPredicate<T, R> alwaysTrue() {
		// TODO make holder
		return new BiPredicate<T, R>() {

			@Override
			public boolean test(T t1, R t2) throws Exception {
				return true;
			}
		};
	}

	public static <T, R> BiPredicate<T, R> alwaysFalse() {
		// TODO make holder
		return new BiPredicate<T, R>() {

			@Override
			public boolean test(T t1, R t2) throws Exception {
				return false;
			}
		};
	}

	public static <T, R> BiPredicate<T, R> throwing() {
		// TODO make holder
		return new BiPredicate<T, R>() {

			@Override
			public boolean test(T t1, R t2) throws Exception {
				throw new ThrowingException();
			}
		};

	}

}
