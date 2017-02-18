package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.Bytes;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.IO;
import com.github.davidmoten.rx2.RetryWhen;
import com.github.davidmoten.rx2.SchedulerHelper;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.functions.Functions;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

public final class FlowableServerSocketTest {

    private static final String LOCALHOST = "127.0.0.1";

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    // private static final int PORT = 12345;
    private static final String TEXT = "hello there";

    private static final int POOL_SIZE = 10;
    private static final Scheduler scheduler = Schedulers.from(Executors.newFixedThreadPool(POOL_SIZE));

    private static final Scheduler clientScheduler = Schedulers.from(Executors.newFixedThreadPool(POOL_SIZE));

    @Test
    public void testCloseQuietly() {
        Socket socket = Mockito.mock(Socket.class);
        FlowableServerSocket.closeQuietly(socket);
    }

    @Test
    public void serverSocketReadsTcpPushWhenBufferIsSmallerThanInput()
            throws UnknownHostException, IOException, InterruptedException {
        checkServerSocketReadsTcpPushWhenBufferSizeIs(TEXT, 4);
    }

    @Test
    public void serverSocketReadsTcpPushWhenBufferIsBiggerThanInput()
            throws UnknownHostException, IOException, InterruptedException {
        checkServerSocketReadsTcpPushWhenBufferSizeIs(TEXT, 8192);
    }

    @Test
    public void serverSocketReadsTcpPushWhenBufferIsSameSizeAsInput()
            throws UnknownHostException, IOException, InterruptedException {
        checkServerSocketReadsTcpPushWhenBufferSizeIs(TEXT, TEXT.length());
    }

    @Test
    public void serverSocketReadsTcpPushWhenInputIsEmpty()
            throws UnknownHostException, IOException, InterruptedException {
        checkServerSocketReadsTcpPushWhenBufferSizeIs("", 4);
    }

    @Test
    public void serverSocketReadsTcpPushWhenInputIsOneCharacter()
            throws UnknownHostException, IOException, InterruptedException {
        checkServerSocketReadsTcpPushWhenBufferSizeIs("a", 4);
    }

    @Test
    public void errorEmittedIfServerSocketBusy() throws IOException {
        reset();
        TestSubscriber<Object> ts = TestSubscriber.create();
        ServerSocket socket = null;
        int port = 12345;
        try {
            socket = new ServerSocket(port);
            IO.serverSocket(port).readTimeoutMs(10000).bufferSize(5).create().subscribe(ts);
            ts.assertNoValues();
            ts.assertNotComplete();
            ts.assertTerminated();
            ts.assertError(new Predicate<Throwable>() {
                @Override
                public boolean test(Throwable e) throws Exception {
                    return e instanceof BindException;
                }
            });
        } finally {
            socket.close();
        }
    }

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(FlowableServerSocket.class);
    }

    @Test
    public void isUtilityClassIO() {
        Asserts.assertIsUtilityClass(IO.class);
    }

    @Test
    public void testCloserWhenDoesNotThrow() throws Exception {
        final AtomicBoolean called = new AtomicBoolean();
        Closeable c = new Closeable() {

            @Override
            public void close() throws IOException {
                called.set(true);
            }
        };
        Consumers.close().accept(c);
        assertTrue(called.get());
    }

    @Test
    public void testCloserWhenThrows() {
        final IOException ex = new IOException();
        Closeable c = new Closeable() {

            @Override
            public void close() throws IOException {
                throw ex;
            }
        };
        try {
            Consumers.close().accept(c);
            Assert.fail();
        } catch (Exception e) {
            assertTrue(ex == e);
        }
    }

    private static void reset() {
        SchedulerHelper.blockUntilWorkFinished(scheduler, POOL_SIZE);
        SchedulerHelper.blockUntilWorkFinished(clientScheduler, POOL_SIZE);
    }

    @Test
    public void testEarlyUnsubscribe() throws UnknownHostException, IOException, InterruptedException {
        reset();
        TestSubscriber<Object> ts = TestSubscriber.create();
        final AtomicReference<byte[]> result = new AtomicReference<byte[]>();
        try {
            int bufferSize = 4;
            AtomicInteger port = new AtomicInteger();
            IO.serverSocketAutoAllocatePort(Consumers.set(port)) //
                    .readTimeoutMs(10000) //
                    .bufferSize(bufferSize) //
                    .create() //
                    .flatMap(new Function<Flowable<byte[]>, Flowable<byte[]>>() {
                        @Override
                        public Flowable<byte[]> apply(Flowable<byte[]> g) {
                            return g //
                                    .firstOrError() //
                                    .toFlowable() //
                                    .doOnNext(Consumers.set(result)) //
                                    .doOnNext(new Consumer<byte[]>() {
                                        @Override
                                        public void accept(byte[] bytes) {
                                            System.out.println(
                                                    Thread.currentThread().getName() + ": " + new String(bytes));
                                        }
                                    }) //
                                    .onErrorResumeNext(Flowable.<byte[]>empty()) //
                                    .subscribeOn(scheduler);
                        }
                    }) //
                    .subscribeOn(scheduler) //
                    .subscribe(ts);
            Thread.sleep(300);
            Socket socket = new Socket(LOCALHOST, port.get());
            OutputStream out = socket.getOutputStream();
            out.write("12345678901234567890".getBytes());
            out.close();
            socket.close();
            Thread.sleep(1000);
            assertEquals("1234", new String(result.get(), UTF_8));
        } finally {
            // will close server socket
            ts.dispose();
        }
    }

    @Test
    public void testCancelDoesNotHaveToWaitForTimeout() throws UnknownHostException, IOException, InterruptedException {
        reset();
        RxJavaPlugins.setErrorHandler(Consumers.printStackTrace());
        TestSubscriber<Object> ts = TestSubscriber.create();
        final AtomicReference<byte[]> result = new AtomicReference<byte[]>();
        AtomicInteger port = new AtomicInteger();
        try {
            int bufferSize = 4;
            IO.serverSocketAutoAllocatePort(Consumers.set(port)) //
                    .readTimeoutMs(Integer.MAX_VALUE).bufferSize(bufferSize).create()
                    .flatMap(new Function<Flowable<byte[]>, Flowable<String>>() {
                        @Override
                        public Flowable<String> apply(Flowable<byte[]> g) {
                            return g //
                                    .firstOrError() //
                                    .toFlowable() //
                                    .doOnNext(Consumers.set(result)) //
                                    .map(new Function<byte[], String>() {
                                        @Override
                                        public String apply(byte[] bytes) {
                                            return new String(bytes, UTF_8);
                                        }
                                    }) //
                                    .doOnNext(new Consumer<String>() {
                                        @Override
                                        public void accept(String s) {
                                            System.out.println(Thread.currentThread().getName() + ": " + s);
                                        }
                                    }) //
                                    .onErrorResumeNext(Flowable.<String>empty()) //
                                    .subscribeOn(scheduler);
                        }
                    }).subscribeOn(scheduler) //
                    .subscribe(ts);
            Thread.sleep(300);
            @SuppressWarnings("resource")
            Socket socket = new Socket(LOCALHOST, port.get());
            OutputStream out = socket.getOutputStream();
            out.write("hell".getBytes(UTF_8));
            out.flush();
            Thread.sleep(500);
            ts.assertValue("hell");
            ts.assertNotTerminated();
            out.write("will-fail".getBytes(UTF_8));
            out.flush();
        } finally {
            // will close server socket
            try {
                ts.dispose();
                Thread.sleep(300);
            } finally {
                RxJavaPlugins.reset();
            }
        }
    }

    @Test
    public void testLoad() throws InterruptedException {
        reset();
        AtomicBoolean errored = new AtomicBoolean(false);
        for (int k = 0; k < 1; k++) {
            System.out.println("loop " + k);
            TestSubscriber<String> ts = TestSubscriber.create();
            final AtomicInteger connections = new AtomicInteger();
            final AtomicInteger port = new AtomicInteger();
            try {
                int bufferSize = 4;
                IO.serverSocketAutoAllocatePort(Consumers.set(port)) //
                        .readTimeoutMs(30000) //
                        .bufferSize(bufferSize) //
                        .create().flatMap(new Function<Flowable<byte[]>, Flowable<byte[]>>() {
                            @Override
                            public Flowable<byte[]> apply(Flowable<byte[]> g) {
                                return g //
                                        .doOnSubscribe(Consumers.increment(connections)) //
                                        .to(Bytes.collect()) //
                                        .toFlowable() //
                                        .doOnError(Consumers.printStackTrace()) //
                                        .subscribeOn(scheduler) //
                                        .retryWhen(RetryWhen.delay(1, TimeUnit.SECONDS).build());
                            }
                        }, 1) //
                        .map(new Function<byte[], String>() {
                            @Override
                            public String apply(byte[] bytes) {
                                return new String(bytes, UTF_8);
                            }
                        }) //
                        .doOnNext(Consumers.decrement(connections)) //
                        .doOnError(Consumers.printStackTrace()) //
                        .doOnError(Consumers.setToTrue(errored)) //
                        .subscribeOn(scheduler) //
                        .subscribe(ts);
                TestSubscriber<Object> ts2 = TestSubscriber.create();
                final Set<String> messages = new ConcurrentSkipListSet<String>();

                final int messageBlocks = 10;
                int numMessages = 1000;

                final AtomicInteger openSockets = new AtomicInteger(0);
                // sender
                Flowable.range(1, numMessages).flatMap(new Function<Integer, Flowable<Object>>() {
                    @Override
                    public Flowable<Object> apply(Integer n) {
                        return Flowable.defer(new Callable<Flowable<Object>>() {
                            @Override
                            public Flowable<Object> call() {
                                // System.out.println(Thread.currentThread().getName()
                                // +
                                // " - writing message");
                                String id = UUID.randomUUID().toString();
                                StringBuilder s = new StringBuilder();
                                for (int i = 0; i < messageBlocks; i++) {
                                    s.append(id);
                                }
                                messages.add(s.toString());
                                Socket socket = null;
                                try {
                                    socket = new Socket(LOCALHOST, port.get());
                                    // allow reuse so we don't run out of
                                    // sockets
                                    socket.setReuseAddress(true);
                                    socket.setSoTimeout(5000);
                                    openSockets.incrementAndGet();
                                    OutputStream out = socket.getOutputStream();
                                    for (int i = 0; i < messageBlocks; i++) {
                                        out.write(id.getBytes(UTF_8));
                                    }
                                    out.close();
                                    openSockets.decrementAndGet();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    try {
                                        if (socket != null)
                                            socket.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                                return Flowable.<Object>just(1);
                            }
                        }) //
                                .timeout(30, TimeUnit.SECONDS) //
                                .subscribeOn(clientScheduler);
                    }
                }) //
                        .doOnError(Consumers.printStackTrace()) //
                        .subscribe(ts2);
                ts2.awaitTerminalEvent();
                ts2.assertComplete();
                // allow server to complete processing
                Thread.sleep(1000);
                ts.assertValueSet(messages);
                assertFalse(errored.get());
            } finally {
                ts.dispose();
                Thread.sleep(1000);
                reset();
            }
        }
    }

    private void checkServerSocketReadsTcpPushWhenBufferSizeIs(String text, int bufferSize)
            throws UnknownHostException, IOException, InterruptedException {
        reset();
        TestSubscriber<Object> ts = TestSubscriber.create();
        final AtomicReference<byte[]> result = new AtomicReference<byte[]>();
        AtomicInteger port = new AtomicInteger();
        try {
            IO.serverSocketAutoAllocatePort(Consumers.set(port)) //
                    .readTimeoutMs(10000) //
                    .bufferSize(bufferSize) //
                    .create() //
                    .flatMap(new Function<Flowable<byte[]>, Flowable<byte[]>>() {
                        @Override
                        public Flowable<byte[]> apply(Flowable<byte[]> g) {
                            return g //
                                    .to(Bytes.collect()) //
                                    .toFlowable() //
                                    .doOnNext(Consumers.set(result)) //
                                    .doOnNext(new Consumer<byte[]>() {
                                        @Override
                                        public void accept(byte[] bytes) {
                                            System.out.println(
                                                    Thread.currentThread().getName() + ": " + new String(bytes));
                                        }
                                    }) //
                                    .onErrorResumeNext(Flowable.<byte[]>empty()) //
                                    .subscribeOn(scheduler);
                        }
                    }).subscribeOn(scheduler) //
                    .subscribe(ts);
            Socket socket = null;
            for (int i = 0; i < 15; i++) {
                Thread.sleep(1000);
                try {
                    socket = new Socket(LOCALHOST, port.get());
                    break;
                } catch (ConnectException e) {
                    System.out.println(e.getMessage());
                }
            }
            assertNotNull("could not connect to port " + port.get(), socket);
            OutputStream out = socket.getOutputStream();
            out.write(text.getBytes());
            out.close();
            socket.close();
            Thread.sleep(1000);
            assertEquals(text, new String(result.get(), UTF_8));
        } finally {
            // will close server socket
            ts.dispose();
        }
    }

    @Test
    public void testAcceptSocketRejectsAlways() throws UnknownHostException, IOException, InterruptedException {
        reset();
        TestSubscriber<Object> ts = TestSubscriber.create();
        try {
            int bufferSize = 4;
            AtomicInteger port = new AtomicInteger();
            IO.serverSocketAutoAllocatePort(Consumers.set(port)) //
                    .readTimeoutMs(10000) //
                    .acceptTimeoutMs(200) //
                    .bufferSize(bufferSize) //
                    .acceptSocketIf(Functions.alwaysFalse()) //
                    .create() //
                    .subscribeOn(scheduler) //
                    .subscribe(ts);
            Thread.sleep(300);
            Socket socket = new Socket(LOCALHOST, port.get());
            OutputStream out = socket.getOutputStream();
            out.write("12345678901234567890".getBytes());
            out.close();
            socket.close();
            Thread.sleep(1000);
            ts.assertNoValues();
        } finally {
            // will close server socket
            ts.dispose();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        reset();
        TestSubscriber<Object> ts = TestSubscriber.create();
        IO.serverSocket(12345).readTimeoutMs(10000).bufferSize(8).create()
                .flatMap(new Function<Flowable<byte[]>, Flowable<byte[]>>() {
                    @Override
                    public Flowable<byte[]> apply(Flowable<byte[]> g) {
                        return g //
                                .to(Bytes.collect()) //
                                .doAfterSuccess(new Consumer<byte[]>() {

                                    @Override
                                    public void accept(byte[] bytes) {
                                        System.out.println(
                                                Thread.currentThread().getName() + ": " + new String(bytes).trim());
                                    }
                                }) //
                                .toFlowable() //
                                .onErrorResumeNext(Flowable.<byte[]>empty()) //
                                .subscribeOn(scheduler);
                    }
                }).subscribeOn(scheduler) //
                .subscribe(ts);

        Thread.sleep(10000000);

    }
}
