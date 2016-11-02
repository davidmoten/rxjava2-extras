package com.github.davidmoten.rx2;

import java.util.concurrent.Callable;

public class Callables {

	private Callables() {
		//prevent instantiation
	}

	public static <T> Callable<T> constant(final T object) {
		return new Callable<T>() {

			@Override
			public T call() throws Exception {
				return object;
			}
		};
	}

}
