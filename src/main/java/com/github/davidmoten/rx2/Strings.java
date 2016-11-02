package com.github.davidmoten.rx2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public final class Strings {

	private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	/**
	 * Returns null if input is null otherwise returns input.toString().trim().
	 */
	private static Function<Object, String> TRIM = new Function<Object, String>() {

		@Override
		public String apply(Object input) throws Exception {
			if (input == null)
				return null;
			else
				return input.toString().trim();
		}
	};

	@SuppressWarnings("unchecked")
	public static <T> Function<T, String> trim() {
		return (Function<T, String>) TRIM;
	}

	public static Flowable<String> from(final Reader reader, final int bufferSize) {
		return Flowable.generate(new Consumer<Emitter<String>>() {
			final char[] buffer = new char[bufferSize];

			@Override
			public void accept(Emitter<String> emitter) throws Exception {
				int count = reader.read(buffer);
				if (count == -1) {
					emitter.onComplete();
				} else {
					emitter.onNext(String.valueOf(buffer, 0, count));
				}
			}
		});
	}

	public static Flowable<String> from(Reader reader) {
		return from(reader, 8192);
	}

	public static Flowable<String> from(InputStream is) {
		return from(new InputStreamReader(is));
	}

	public static Flowable<String> from(InputStream is, Charset charset) {
		return from(new InputStreamReader(is, charset));
	}

	public static Flowable<String> from(InputStream is, Charset charset, int bufferSize) {
		return from(new InputStreamReader(is, charset), bufferSize);
	}

	public static Flowable<String> split(Flowable<String> source, String pattern) {
		return source.compose(Transformers.split(pattern,BackpressureStrategy.BUFFER, 1));
	}

	public static Maybe<String> concat(Flowable<String> source) {
		return join(source, "");
	}

	public static Maybe<String> concat(Flowable<String> source, final String delimiter) {
		return join(source, delimiter);
	}

	public static Flowable<String> strings(Flowable<?> source) {
		return source.map(new Function<Object, String>() {
			@Override
			public String apply(Object t) throws Exception {
				return String.valueOf(t);
			}
		});
	}

	public static Flowable<String> from(File file) {
		return from(file, DEFAULT_CHARSET);
	}

	public static Flowable<String> from(final File file, final Charset charset) {
		Preconditions.checkNotNull(file);
		Preconditions.checkNotNull(charset);
		Callable<Reader> resourceFactory = new Callable<Reader>() {
			@Override
			public Reader call() throws FileNotFoundException {
				return new InputStreamReader(new FileInputStream(file), charset);
			}
		};
		return from(resourceFactory);
	}

	public static Flowable<String> fromClasspath(final Class<?> cls, final String resource, final Charset charset) {
		Preconditions.checkNotNull(resource);
		Preconditions.checkNotNull(charset);
		Callable<Reader> resourceFactory = new Callable<Reader>() {
			@Override
			public Reader call() {
				return new InputStreamReader(cls.getResourceAsStream(resource), charset);
			}
		};
		return from(resourceFactory);
	}
	
	public static Flowable<String> fromClasspath(final String resource, final Charset charset) {
		return fromClasspath(Strings.class, resource, charset);
	}

	public static Flowable<String> fromClasspath(final String resource) {
		return fromClasspath(resource, Utf8Holder.INSTANCE);
	}

	public static Flowable<String> from(final Callable<Reader> readerFactory) {
		Function<Reader, Flowable<String>> flowableFactory = new Function<Reader, Flowable<String>>() {
			@Override
			public Flowable<String> apply(Reader reader) {
				return from(reader);
			}
		};
		return Flowable.using(readerFactory, flowableFactory, DisposeActionHolder.INSTANCE, true);
	}

	public static Maybe<String> join(Flowable<String> source) {
		return join(source, "");
	}

	public static Flowable<String> decode(Flowable<byte[]> source, CharsetDecoder decoder) {
		return source.compose(Transformers.decode(decoder));
	}

	public static Flowable<String> decode(Flowable<byte[]> source, Charset charset) {
		return decode(source, charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE));
	}

	public static Flowable<String> decode(Flowable<byte[]> source, String charset) {
		return decode(source, Charset.forName(charset));
	}

	public static Maybe<String> join(final Flowable<String> source, final String delimiter) {

		return Maybe.defer(new Callable<Maybe<String>>() {
			final AtomicBoolean afterFirst = new AtomicBoolean(false);
			final AtomicBoolean isEmpty = new AtomicBoolean(true);

			@Override
			public Maybe<String> call() {
				return source.collect(new Callable<StringBuilder>() {
					@Override
					public StringBuilder call() {
						return new StringBuilder();
					}
				}, new BiConsumer<StringBuilder, String>() {

					@Override
					public void accept(StringBuilder b, String s) throws Exception {
						if (!afterFirst.compareAndSet(false, true)) {
							b.append(delimiter);
						}
						b.append(s);
						isEmpty.set(false);

					}
				}).flatMapMaybe(new Function<StringBuilder, Maybe<String>>() {

					@Override
					public Maybe<String> apply(StringBuilder b) {
						if (isEmpty.get())
							return Maybe.empty();
						else
							return Maybe.just(b.toString());
					}
				});

			}
		});
	}

	public static Flowable<List<String>> splitLines(InputStream is, Charset charset, final String delimiter,
			final String commentPrefix) {
		return from(is, charset).compose(Transformers.split("\n", BackpressureStrategy.BUFFER, 1)) //
				.filter(new Predicate<String>() {
					@Override
					public boolean test(String line) {
						return !line.startsWith(commentPrefix);
					}
				}) //
				.map(SplitLinesHolder.trim) //
				.filter(SplitLinesHolder.notEmpty) //
				.map(new Function<String, List<String>>() {
					@Override
					public List<String> apply(String line) {
						return Arrays.asList(line.split(delimiter));
					}
				});
	}

	public static Flowable<List<String>> splitLines(InputStream is, String delimiter) {
		return splitLines(is, DEFAULT_CHARSET, delimiter, "#");
	}

	private static class Utf8Holder {
		static final Charset INSTANCE = Charset.forName("UTF-8");
	}
	
	private static class DisposeActionHolder {
		static final Consumer<Reader> INSTANCE = new Consumer<Reader>() {
			@Override
			public void accept(Reader reader) throws IOException {
				reader.close();
			}
		};
	}
	private static class SplitLinesHolder {
		static final Function<String, String> trim = new Function<String, String>() {
			@Override
			public String apply(String line) {
				return line.trim();
			}
		};
		static final Predicate<String> notEmpty = new Predicate<String>() {
			@Override
			public boolean test(String line) {
				return !line.isEmpty();
			}
		};
	}

}
