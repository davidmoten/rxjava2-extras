package com.github.davidmoten.rx2.buffertofile;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;

import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;
import com.github.davidmoten.rx2.internal.flowable.buffertofile.FlowableOnBackpressureBufferToFile;

import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

public final class Options {

    public static final String DEFAULT_FILE_PREFIX = "bufferToFile_";

    private final Callable<File> fileFactory;
    private final int pageSizeBytes;
    private final Scheduler scheduler;

    @VisibleForTesting
    Options(Callable<File> filefactory, int pageSizeBytes, Scheduler scheduler) {
        Preconditions.checkNotNull(filefactory);
        Preconditions.checkArgument(pageSizeBytes > 0, "bufferSizeBytes must be greater than 0");
        Preconditions.checkNotNull(scheduler);
        this.fileFactory = filefactory;
        this.pageSizeBytes = pageSizeBytes;
        this.scheduler = scheduler;
    }

    public Callable<File> fileFactory() {
        return fileFactory;
    }

    public int pageSizeBytes() {
        return pageSizeBytes;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public static BuilderFlowable builderFlowable() {
        return new BuilderFlowable();
    }

    public static BuilderObservable builderObservable() {
        return new BuilderObservable();
    }

    public static final class BuilderFlowable {

        private Callable<File> fileFactory = FileFactoryHolder.INSTANCE;
        private int pageSizeBytes = 1024 * 1024;
        private Scheduler scheduler = Schedulers.io();

        BuilderFlowable() {
        }

        /**
         * Sets the page size in bytes. A page corresponds to a single memory
         * mapped file. If this method is not called the default value is 1MB
         * (1024*1024 bytes).
         * 
         * @param pageSizeBytes
         *            the page size in bytes.
         * @return this
         */
        public BuilderFlowable pageSizeBytes(int pageSizeBytes) {
            this.pageSizeBytes = pageSizeBytes;
            return this;
        }

        /**
         * Sets the scheduler to use for reading items from files and emitting
         * them.
         * 
         * @param scheduler
         *            for emitting items
         * @return this
         */
        public BuilderFlowable scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        /**
         * Sets the file factory to be used by the queue storage mechanism.
         * Defaults to using {@code File.createTempFile("bufferToFileDb","")} if
         * this method is not called.
         * 
         * @param fileFactory
         *            the factory
         * @return the current builder
         */
        public BuilderFlowable fileFactory(Callable<File> fileFactory) {
            this.fileFactory = fileFactory;
            return this;
        }

        /**
         * Returns a Flowable Transformer to buffer stream items to disk
         * (files). The serialization method uses
         * {@code java.io.ObjectOutputStream} for writing values to disk and
         * {@code java.io.ObjectInputStream} for reading values from disk and
         * emitting them. Note that this is a pretty verbose serialization
         * method for small items in that a 4 byte Integer is serialized to an
         * 81 byte array!
         * 
         * @param <T>
         *            stream item type
         * @return FlowableTransformer using the java io serializer
         */
        public <T extends Serializable> FlowableTransformer<T, T> serializerJavaIO() {
            return serializer(Serializers.<T>javaIO());
        }

        /**
         * Returns a Flowable Transformer that buffers items to disk (files).
         * The serialization method passes through byte arrays directly for both
         * writing and reading.
         * 
         * @return FlowableTransformer using the bytes serializer
         */
        public FlowableTransformer<byte[], byte[]> serializerBytes() {
            return serializer(Serializers.bytes());
        }

        public FlowableTransformer<String, String> serializerUtf8() {
            return serializer(Serializers.utf8());
        }

        public <T> FlowableTransformer<T, T> serializer(final Serializer<T> serializer) {
            final Options options = new Options(fileFactory, pageSizeBytes, scheduler);
            return new FlowableTransformer<T, T>() {

                @Override
                public Publisher<T> apply(Flowable<T> source) {
                    return new FlowableOnBackpressureBufferToFile<T>(source, null, options, serializer);
                }
            };
        }

        public <T> FlowableTransformer<T, T> serializer(DataSerializer<T> ds) {
            return serializer(Serializers.from(ds));
        }
    }

    public static final class BuilderObservable {

        private Callable<File> fileFactory = FileFactoryHolder.INSTANCE;
        private int pageSizeBytes = 1024 * 1024;
        private Optional<Scheduler> scheduler = Optional.absent();

        BuilderObservable() {
        }

        /**
         * Sets the page size in bytes. A page corresponds to a single memory
         * mapped file. If this method is not called the default value is 1MB
         * (1024*1024 bytes).
         * 
         * @param pageSizeBytes
         *            the page size in bytes.
         * @return this
         */
        public BuilderObservable pageSizeBytes(int pageSizeBytes) {
            this.pageSizeBytes = pageSizeBytes;
            return this;
        }

        /**
         * Sets the scheduler to use for reading items from files and emitting
         * them.
         * 
         * @param scheduler
         *            for emitting items
         * @return this
         */
        public BuilderObservable scheduler(Scheduler scheduler) {
            this.scheduler = Optional.of(scheduler);
            return this;
        }

        /**
         * Sets the file factory to be used by the queue storage mechanism.
         * Defaults to using {@code File.createTempFile("bufferToFileDb","")} if
         * this method is not called.
         * 
         * @param fileFactory
         *            the factory
         * @return the current builder
         */
        public BuilderObservable fileFactory(Callable<File> fileFactory) {
            this.fileFactory = fileFactory;
            return this;
        }

        /**
         * Returns a Flowable Transformer to buffer stream items to disk
         * (files). The serialization method uses
         * {@code java.io.ObjectOutputStream} for writing values to disk and
         * {@code java.io.ObjectInputStream} for reading values from disk and
         * emitting them. Note that this is a pretty verbose serialization
         * method for small items in that a 4 byte Integer is serialized to an
         * 81 byte array!
         * 
         * @param <T>
         *            stream item type
         * @return FlowableTransformer using the java io serializer
         */
        public <T extends Serializable> Function<Observable<T>, Flowable<T>> serializerJavaIO() {
            return serializer(Serializers.<T>javaIO());
        }

        /**
         * Returns a Flowable Transformer that buffers items to disk (files).
         * The serialization method passes through byte arrays directly for both
         * writing and reading.
         * 
         * @return FlowableTransformer using the bytes serializer
         */
        public Function<Observable<byte[]>, Flowable<byte[]>> serializerBytes() {
            return serializer(Serializers.bytes());
        }

        public Function<Observable<String>, Flowable<String>> serializerUtf8() {
            return serializer(Serializers.utf8());
        }

        public <T> Function<Observable<T>, Flowable<T>> serializer(final Serializer<T> serializer) {
            Scheduler s = scheduler.isPresent() ? scheduler.get() : Schedulers.io();
            final Options options = new Options(fileFactory, pageSizeBytes, s);
            return new Function<Observable<T>, Flowable<T>>() {

                @Override
                public Flowable<T> apply(Observable<T> source) {
                    return new FlowableOnBackpressureBufferToFile<T>(null, source, options, serializer);
                }
            };
        }

        public <T> Function<Observable<T>, Flowable<T>> serializer(DataSerializer<T> ds) {
            return serializer(Serializers.from(ds));
        }
    }

    @VisibleForTesting
    static final class FileFactoryHolder {
        private static final Callable<File> INSTANCE = new Callable<File>() {
            @Override
            public File call() {
                try {
                    return Files.createTempFile(DEFAULT_FILE_PREFIX, ".obj").toFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

}
