package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;

import com.github.davidmoten.rx2.Callables;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;
import com.github.davidmoten.rx2.Flowables;
import com.github.davidmoten.rx2.ObservableTransformers;
import com.github.davidmoten.rx2.buffertofile.Options.BuilderFlowable;
import com.github.davidmoten.rx2.buffertofile.Options.BuilderObservable;
import com.github.davidmoten.rx2.buffertofile.Serializer;
import com.github.davidmoten.rx2.buffertofile.Serializers;

import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FlowableOnBackpressureBufferToFileTest {

    private static final Callable<File> FILE_FACTORY = new Callable<File>() {
        @Override
        public File call() throws Exception {
            return File.createTempFile("bufferToFile", ".page", new File("target"));
        }
    };

    private static final long N = Long.parseLong(System.getProperty("n", 8 * 1024 * 1024 + ""));

    @Test
    public void testJavaIOSerializable() {
        Flowable.just(1, 2, 3) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(1000000) //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5000000000L, TimeUnit.SECONDS) //
                .assertValues(1, 2, 3)//
                .assertComplete();
    }

    @Test
    public void testJavaIOSerializableObservableSource() {
        Observable.just(1, 2, 3) //
                .to(onBackpressureBufferToFileObservable() //
                        .pageSizeBytes(1000000) //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5000000000L, TimeUnit.SECONDS) //
                .assertValues(1, 2, 3)//
                .assertComplete();
    }

    private static BuilderFlowable onBackpressureBufferToFile() {
        return FlowableTransformers. //
                onBackpressureBufferToFile() //
                .fileFactory(FILE_FACTORY);
    }

    private static BuilderObservable onBackpressureBufferToFileObservable() {
        return ObservableTransformers. //
                onBackpressureBufferToFile() //
                .fileFactory(FILE_FACTORY);
    }

    @Test
    public void testByteArrayLengthOne() {
        byte[] bytes = new byte[] { 3 };
        Flowable.just(bytes) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(1000000) //
                        .serializerBytes()) //
                .doOnNext(Consumers.assertBytesEquals(bytes)) //
                .doOnError(Consumers.printStackTrace()) //
                .test() //
                .awaitDone(500000000L, TimeUnit.SECONDS) //
                .assertValueCount(1) //
                .assertComplete();
    }

    @Test
    public void testByteArrays() {
        byte[] bytes = new byte[] { 1, 2, 3 };
        Flowable.just(bytes) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(1000000) //
                        .serializerBytes()) //
                .doOnNext(Consumers.assertBytesEquals(bytes)) //
                .doOnError(Consumers.printStackTrace()) //
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertValueCount(1) //
                .assertComplete();
    }

    @Test
    public void testEmpty() {
        Flowable.<Integer> empty() //
                .compose(onBackpressureBufferToFile() //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertComplete(); //
    }

    @Test
    public void testOne() {
        Flowable.just(4) //
                .compose(onBackpressureBufferToFile() //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertValue(4) //
                .assertComplete(); //
    }

    @Test
    public void testOneWithSerializerThatReturnsNullAtSerialize() {
        Flowable.just(4) //
                .compose(onBackpressureBufferToFile() //
                        .serializer(new Serializer<Integer>() {

                            @Override
                            public byte[] serialize(Integer t) throws IOException {
                                return null;
                            }

                            @Override
                            public Integer deserialize(byte[] bytes)
                                    throws IOException, ClassNotFoundException {
                                return null;
                            }
                        }))
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(NullPointerException.class); //
    }

    @Test
    public void testOneWithSerializerThatReturnsNullAtDeserialize() {
        Flowable.just(4) //
                .compose(onBackpressureBufferToFile() //
                        .serializer(new Serializer<Integer>() {

                            @Override
                            public byte[] serialize(Integer t) throws IOException {
                                return Serializers.javaIO().serialize(t);
                            }

                            @Override
                            public Integer deserialize(byte[] bytes)
                                    throws IOException, ClassNotFoundException {
                                return null;
                            }
                        }))
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(NullPointerException.class); //
    }

    @Test(expected = NullPointerException.class)
    public void testNullSubscriber() {
        Flowable.just(1).compose(onBackpressureBufferToFile().<Integer> serializerJavaIO())
                .subscribe((Subscriber<Integer>) null);
    }

    @Test
    public void testError() {
        IOException e = new IOException();
        Flowable.<Integer> error(e) //
                .compose(onBackpressureBufferToFile() //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(e); //
    }

    @Test
    public void testErrorObservable() {
        IOException e = new IOException();
        Observable.<Integer> error(e) //
                .to(onBackpressureBufferToFileObservable() //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(e); //
    }

    @Test
    public void testItemsThenError() {
        IOException e = new IOException();
        Flowable.just(1, 2, 3).concatWith(Flowable.<Integer> error(e)) //
                .compose(onBackpressureBufferToFile() //
                        .<Integer> serializerJavaIO())
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertError(e); //
    }

    @Test
    public void testMessageCrossesPage() {
        byte[] bytes = new byte[] { 1, 2, 3, 4, 5, 6 };
        // length field + padding field + padding + bytes = 4 + 1 + 1 + 6 = 12
        // bytes
        Flowable.just(bytes) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(64) //
                        .serializerBytes()) //
                .doOnNext(Consumers.assertBytesEquals(bytes)) //
                .doOnError(Consumers.printStackTrace()) //
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertValueCount(1) //
                .assertComplete();
    }

    @Test
    public void testOneMessagePerPage() {
        int n = 20;
        // each integer takes 81 bytes of message plus 8 bytes of header
        // using javaIO serialization so remaining space per page is 11 bytes
        // which will be padding in the FULL_MESSAGE.
        Flowable.range(1, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(100) //
                        .<Integer> serializerJavaIO()) //
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValueCount(n);
    }

    @Test
    public void testManyIntegers() {
        int n = 200;
        // each integer takes 81 bytes of message plus 8 bytes of header
        // using javaIO serialization
        Flowable.range(1, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(1000) //
                        .<Integer> serializerJavaIO()) //
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValueCount(n);
    }

    @Test
    public void testVeryManyIntegers() {
        int n = 200000;
        // each integer takes 81 bytes of message plus 8 bytes of header
        // using javaIO serialization
        Flowable.range(1, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(10000000) //
                        .<Integer> serializerJavaIO()) //
                .count() //
                .test() //
                .awaitDone(50L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
    }

    @Test
    public void testVeryManyByteArrays() {
        long n = N;
        byte[] bytes = new byte[40];
        new Random().nextBytes(bytes);
        long t = System.currentTimeMillis();
        Flowables.repeat(bytes, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(20000000) //
                        .serializerBytes()) //
                .count() //
                .test() //
                .awaitDone(500L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
        t = (System.currentTimeMillis() - t);
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("byte arrays async rate = "
                + df.format((1000.0 * bytes.length * n / 1024.0 / 1024.0 / t)) + "MB/s, "
                + "msgs/sec = " + df.format(n * 1000.0 / t));
    }

    @Test
    public void testVeryManyByteArraysObservableSource() {
        long n = N;
        byte[] bytes = new byte[40];
        new Random().nextBytes(bytes);
        long t = System.currentTimeMillis();
        Observable.just(bytes) //
                .repeat(n) //
                .to(onBackpressureBufferToFileObservable() //
                        .pageSizeBytes(20000000) //
                        .serializerBytes()) //
                .count() //
                .test() //
                .awaitDone(500L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
        t = (System.currentTimeMillis() - t);
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("byte arrays async rate obs = "
                + df.format((1000.0 * bytes.length * n / 1024.0 / 1024.0 / t)) + "MB/s, "
                + "msgs/sec = " + df.format(n * 1000.0 / t));
    }

    @Test
    public void testCancelDeletesPageFileWhenNoOutstandingRequests() {
        long n = 50L;
        byte[] bytes = new byte[40];
        new Random().nextBytes(bytes);
        File pageFile = new File("target/bufferToFile-1");
        TestSubscriber<byte[]> ts = Flowables.repeat(bytes, n) //
                .compose(onBackpressureBufferToFile() //
                        .fileFactory(Callables.constant(pageFile)) //
                        .scheduler(Schedulers.trampoline()) //
                        .pageSizeBytes(20000000) //
                        .serializerBytes()) //
                .test(1) //
                .assertNoErrors() //
                .assertNotComplete();
        ts.cancel();
        assertFalse(pageFile.exists());
    }

    @Test
    public void testFragments() {
        System.out.println("testing fragments sync");
        System.out.println(ByteOrder.nativeOrder().toString());
        for (int i = 1; i <= 500; i++) {
            checkFragments(i, 160, 1, Schedulers.trampoline());
        }
    }

    @Test
    @Ignore
    public void testFragmentsAsync() {
        System.out.println("testing fragments async");
        for (int i = 1; i <= 500; i++) {
            checkFragments(i, 160, 10, Schedulers.io());
        }
    }

    private void checkFragments(int bytesSize, int pageSize, int numRuns, Scheduler scheduler) {
        byte[] bytes = new byte[bytesSize];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        // length field + padding field + padding + bytes = 4 + 1 + 1 + 300 =
        // 306 bytes
        for (int n = 1; n <= numRuns; n++) {
//            System.out.println("bytes=" + bytesSize + " ======== " + n + " =========");
            Flowables.repeat(bytes, n) //
                    .compose(onBackpressureBufferToFile() //
                            .pageSizeBytes(pageSize) //
                            .scheduler(scheduler) //
                            .serializerBytes()) //
                    .doOnNext(Consumers.assertBytesEquals(bytes)) //
                    // .doOnNext(Consumers.println()) //
                    .doOnError(Consumers.printStackTrace()) //
                    .test() //
                    .awaitDone(50000000L, TimeUnit.SECONDS) //
                    .assertValueCount(n) //
                    .assertComplete();
        }
    }

    @Test
    public void testSynchronous() {
        long n = N;
        byte[] bytes = new byte[40];
        new Random().nextBytes(bytes);
        long t = System.currentTimeMillis();
        Flowables.repeat(bytes, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(48000000) //
                        .scheduler(Schedulers.trampoline()) //
                        .serializerBytes()) //
                .count() //
                .test() //
                .awaitDone(500L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("byte arrays sync rate = " + df.format(
                (1000.0 * bytes.length * n / 1024.0 / 1024.0 / (System.currentTimeMillis() - t)))
                + "MB/s");

    }

    @Test
    public void testCancel() throws InterruptedException {
        System.out.println("testCancel");
        for (int i = 0; i < 50; i++) {
            // System.out.println(i);
            long n = 5000L;
            File pageFile = new File("target/cancel" + i + ".obj");
            pageFile.delete();
            byte[] bytes = new byte[40];
            Flowables.repeat(bytes, n) //
                    .compose(FlowableTransformers //
                            .onBackpressureBufferToFile() //
                            .fileFactory(Callables.constant(pageFile)) //
                            .pageSizeBytes(20000000) //
                            .serializerBytes()) //
                    .take(5) //
                    .count() //
                    .test() //
                    .awaitDone(500L, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertComplete() //
                    .assertValue((long) 5);
            long t = TimeUnit.SECONDS.toMillis(5);
            while (t > 0 && pageFile.exists()) {
                long waitMs = 10;
                TimeUnit.MILLISECONDS.sleep(waitMs);
                t -= waitMs;
            }
            assertFalse(pageFile.exists());
        }
    }

    @Test
    public void testCancelObservable() throws InterruptedException {
        System.out.println("testCancelObservable");
        for (int i = 0; i < 50; i++) {
            // System.out.println(i);
            long n = 5000L;
            File pageFile = new File("target/cancelObs" + i + ".obj");
            pageFile.delete();
            byte[] bytes = new byte[40];
            Observable.just(bytes) //
                    .repeat(n) //
                    .to(ObservableTransformers //
                            .onBackpressureBufferToFile() //
                            .fileFactory(Callables.constant(pageFile)) //
                            .pageSizeBytes(20000000) //
                            .serializerBytes()) //
                    .take(5) //
                    .count() //
                    .test() //
                    .awaitDone(500L, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertComplete() //
                    .assertValue((long) 5);
            long t = TimeUnit.SECONDS.toMillis(5);
            while (t > 0 && pageFile.exists()) {
                long waitMs = 10;
                TimeUnit.MILLISECONDS.sleep(waitMs);
                t -= waitMs;
            }
            assertFalse(pageFile.exists());
        }
    }

    @Test
    public void testVeryManyByteArraysBatchedRequests() {
        long n = N;
        byte[] bytes = new byte[40];
        new Random().nextBytes(bytes);
        long t = System.currentTimeMillis();
        Flowables.repeat(bytes, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(20000000) //
                        .serializerBytes()) //
                .rebatchRequests(8192) //
                .count() //
                .test() //
                .awaitDone(500L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
        t = (System.currentTimeMillis() - t);
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("byte arrays async rebatched rate = "
                + df.format((1000.0 * bytes.length * n / 1024.0 / 1024.0 / t)) + "MB/s, "
                + "msgs/sec=" + df.format(n * 1000.0 / t));
    }

    @Test
    public void testVeryMany1KByteArrays() {
        long n = N;
        byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);
        long t = System.currentTimeMillis();
        Flowables.repeat(bytes, n) //
                .compose(onBackpressureBufferToFile() //
                        .pageSizeBytes(20000000) //
                        .serializerBytes()) //
                .count() //
                .test() //
                .awaitDone(500L, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertComplete() //
                .assertValue((long) n);
        t = (System.currentTimeMillis() - t);
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("byte arrays async 1K rate = "
                + df.format((1000.0 * bytes.length * n / 1024.0 / 1024.0 / t)) + "MB/s, "
                + "msgs/sec = " + df.format(n * 1000.0 / t));
    }

    @Test
    public void testCloseQueue() {
        PagedQueue queue = Mockito.mock(PagedQueue.class);
        Mockito.doNothing().when(queue).close();
        FlowableOnBackpressureBufferToFile.close(queue);
        Mockito.verify(queue, Mockito.atLeastOnce()).close();
    }
    
    @Test
    public void testCloseQueueThrowsExceptionReportsToPlugins() {
        AtomicBoolean set = new AtomicBoolean();
        RxJavaPlugins.setErrorHandler(Consumers.<Throwable>setToTrue(set));
        PagedQueue queue = Mockito.mock(PagedQueue.class);
        RuntimeException err = new RuntimeException();
        Mockito.doThrow(err).when(queue).close();
        FlowableOnBackpressureBufferToFile.close(queue);
        Mockito.verify(queue, Mockito.atLeastOnce()).close();
        assertTrue(set.get());
    }
}
