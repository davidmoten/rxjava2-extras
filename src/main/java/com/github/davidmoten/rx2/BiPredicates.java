package com.github.davidmoten.rx2;

import io.reactivex.functions.BiPredicate;

public final  class BiPredicates {

	public static <T,R> BiPredicate<T,R> alwaysTrue() {
		//TODO make holder
		return new BiPredicate<T,R>() {

			@Override
			public boolean test(T t1, R t2) throws Exception {
				return true;
			}};
	}

	public static <T,R> BiPredicate<T,R> alwaysFalse() {
		//TODO make holder
		return new BiPredicate<T,R>() {

			@Override
			public boolean test(T t1, R t2) throws Exception {
				return false;
			}};
	}
	
	
}
