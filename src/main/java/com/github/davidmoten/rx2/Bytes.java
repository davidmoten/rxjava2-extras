package com.github.davidmoten.rx2;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.github.davidmoten.rx2.util.ZippedEntry;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public final class Bytes {

	private static final int DEFAULT_BUFFER_SIZE = 8192;

	private Bytes() {
		// prevent instantiation
	}

	/**
	 * Returns a Flowable stream of byte arrays from the given
	 * {@link InputStream} between 1 and {@code bufferSize} bytes.
	 * 
	 * @param is
	 *            input stream of bytes
	 * @param bufferSize
	 *            max emitted byte array size
	 * @return a stream of byte arrays
	 */
	public static Flowable<byte[]> from(final InputStream is, final int bufferSize) {
		return Flowable.generate(new Consumer<Emitter<byte[]>>() {
			@Override
			public void accept(Emitter<byte[]> emitter) throws Exception {
				byte[] buffer = new byte[bufferSize];
				int count = is.read(buffer);
				if (count == -1) {
					emitter.onComplete();
				} else if (count < bufferSize) {
					emitter.onNext(Arrays.copyOf(buffer, count));
				} else {
					emitter.onNext(buffer);
				}
			}
		});
	}

	public static Flowable<byte[]> from(File file) {
		return from(file, 8192);
	}

	public static Flowable<byte[]> from(final File file, final int size) {
		Callable<InputStream> resourceFactory = new Callable<InputStream>() {

			@Override
			public InputStream call() throws FileNotFoundException {
				return new BufferedInputStream(new FileInputStream(file), size);
			}
		};
		Function<InputStream, Flowable<byte[]>> observableFactory = new Function<InputStream, Flowable<byte[]>>() {

			@Override
			public Flowable<byte[]> apply(InputStream is) {
				return from(is, size);
			}
		};
		return Flowable.using(resourceFactory, observableFactory, InputStreamCloseHolder.INSTANCE, true);
	}

	private static final class InputStreamCloseHolder {
		static final Consumer<InputStream> INSTANCE = new Consumer<InputStream>() {

			@Override
			public void accept(InputStream is) throws IOException {
				is.close();
			}
		};
	}

	/**
	 * Returns an Flowable stream of byte arrays from the given
	 * {@link InputStream} of {@code 8192} bytes. The final byte array may be
	 * less than {@code 8192} bytes.
	 * 
	 * @param is
	 *            input stream of bytes
	 * @return a stream of byte arrays
	 */
	public static Flowable<byte[]> from(InputStream is) {
		return from(is, DEFAULT_BUFFER_SIZE);
	}

	public static Flowable<ZippedEntry> unzip(final File file) {
		Callable<ZipInputStream> resourceFactory = new Callable<ZipInputStream>() {
			@Override
			public ZipInputStream call() throws FileNotFoundException {
				return new ZipInputStream(new FileInputStream(file));
			}
		};
		Function<ZipInputStream, Flowable<ZippedEntry>> observableFactory = ZipHolder.OBSERVABLE_FACTORY;
		Consumer<ZipInputStream> disposeAction = ZipHolder.DISPOSER;
		return Flowable.using(resourceFactory, observableFactory, disposeAction);
	}

	public static Flowable<ZippedEntry> unzip(final InputStream is) {
		return unzip(new ZipInputStream(is));
	}

	public static Flowable<ZippedEntry> unzip(final ZipInputStream zis) {

		return Flowable.generate(new Consumer<Emitter<ZippedEntry>>() {
			@Override
			public void accept(Emitter<ZippedEntry> emitter) throws IOException {
				ZipEntry zipEntry = zis.getNextEntry();
				if (zipEntry != null) {
					emitter.onNext(new ZippedEntry(zipEntry, zis));
				} else {
					// end of stream so eagerly close the stream (might not be a
					// good idea since this method did not create the zis
					zis.close();
					emitter.onComplete();
				}
			}
		});

	}

	public static Single<byte[]> collect(Flowable<byte[]> source) {
		return source.collect(BosCreatorHolder.INSTANCE, BosCollectorHolder.INSTANCE).map(BosToArrayHolder.INSTANCE);
	}

	public static Function<Flowable<byte[]>, Single<byte[]>> collect() {
		return new Function<Flowable<byte[]>, Single<byte[]>>() {
			@Override
			public Single<byte[]> apply(Flowable<byte[]> source) throws Exception {
				return collect(source);
			}
		};
	}

	private static final class BosCreatorHolder {
		static final Callable<ByteArrayOutputStream> INSTANCE = new Callable<ByteArrayOutputStream>() {

			@Override
			public ByteArrayOutputStream call() {
				return new ByteArrayOutputStream();
			}
		};
	}

	private static final class BosCollectorHolder {
		static final BiConsumer<ByteArrayOutputStream, byte[]> INSTANCE = new BiConsumer<ByteArrayOutputStream, byte[]>() {

			@Override
			public void accept(ByteArrayOutputStream bos, byte[] bytes) throws IOException {
				bos.write(bytes);
			}
		};
	}

	private static final class BosToArrayHolder {
		static final Function<ByteArrayOutputStream, byte[]> INSTANCE = new Function<ByteArrayOutputStream, byte[]>() {
			@Override
			public byte[] apply(ByteArrayOutputStream bos) {
				return bos.toByteArray();
			}
		};
	}

	private static final class ZipHolder {
		static final Consumer<ZipInputStream> DISPOSER = new Consumer<ZipInputStream>() {

			@Override
			public void accept(ZipInputStream zis) throws IOException {
				zis.close();
			}
		};
		final static Function<ZipInputStream, Flowable<ZippedEntry>> OBSERVABLE_FACTORY = new Function<ZipInputStream, Flowable<ZippedEntry>>() {
			@Override
			public Flowable<ZippedEntry> apply(ZipInputStream zis) {
				return unzip(zis);
			}
		};
	}

}
