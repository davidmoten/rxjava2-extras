package com.github.davidmoten.rx2.internal.flowable;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Callable;

import com.github.davidmoten.guavamini.annotations.VisibleForTesting;
import com.github.davidmoten.rx2.Bytes;
import com.github.davidmoten.rx2.Consumers;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public final class FlowableServerSocket {

    private FlowableServerSocket() {
        // prevent instantiation
    }

    public static Flowable<Flowable<byte[]>> create(
            final Callable<? extends ServerSocket> serverSocketFactory, final int timeoutMs,
            final int bufferSize, Action preAcceptAction, int acceptTimeoutMs,
            Predicate<? super Socket> acceptSocket) {
        Function<ServerSocket, Flowable<Flowable<byte[]>>> FlowableFactory = createFlowableFactory(
                timeoutMs, bufferSize, preAcceptAction, acceptSocket);
        return Flowable.<Flowable<byte[]>, ServerSocket> using( //
                createServerSocketFactory(serverSocketFactory, acceptTimeoutMs), //
                FlowableFactory, //
                new Consumer<ServerSocket>() {
                    // Note that in java 1.6, ServerSocket does not implement
                    // Closeable
                    @Override
                    public void accept(ServerSocket ss) throws Exception {
                        ss.close();
                    }
                }, //
                true);
    }

    private static Callable<ServerSocket> createServerSocketFactory(
            final Callable<? extends ServerSocket> serverSocketFactory, final int acceptTimeoutMs) {
        return new Callable<ServerSocket>() {
            @Override
            public ServerSocket call() throws Exception {
                return createServerSocket(serverSocketFactory, acceptTimeoutMs);
            }
        };
    }

    private static ServerSocket createServerSocket(
            Callable<? extends ServerSocket> serverSocketCreator, long timeoutMs) throws Exception {
        ServerSocket s = serverSocketCreator.call();
        s.setSoTimeout((int) timeoutMs);
        return s;
    }

    private static Function<ServerSocket, Flowable<Flowable<byte[]>>> createFlowableFactory(
            final int timeoutMs, final int bufferSize, final Action preAcceptAction,
            final Predicate<? super Socket> acceptSocket) {
        return new Function<ServerSocket, Flowable<Flowable<byte[]>>>() {
            @Override
            public Flowable<Flowable<byte[]>> apply(ServerSocket serverSocket) {
                return createServerSocketFlowable(serverSocket, timeoutMs, bufferSize,
                        preAcceptAction, acceptSocket);
            }
        };
    }

    private static Flowable<Flowable<byte[]>> createServerSocketFlowable(
            final ServerSocket serverSocket, final long timeoutMs, final int bufferSize,
            final Action preAcceptAction, final Predicate<? super Socket> acceptSocket) {
        return Flowable.generate( //
                new Consumer<Emitter<Flowable<byte[]>>>() {
                    @Override
                    public void accept(Emitter<Flowable<byte[]>> emitter) throws Exception {
                        acceptConnection(timeoutMs, bufferSize, serverSocket, emitter,
                                preAcceptAction, acceptSocket);
                    }
                });
    }

    private static void acceptConnection(long timeoutMs, int bufferSize, ServerSocket ss,
            Emitter<Flowable<byte[]>> emitter, Action preAcceptAction,
            Predicate<? super Socket> acceptSocket) {
        Socket socket;
        while (true) {
            try {
                preAcceptAction.run();
                socket = ss.accept();
                if (!acceptSocket.test(socket)) {
                    closeQuietly(socket);
                } else {
                    emitter.onNext(createSocketFlowable(socket, timeoutMs, bufferSize));
                    break;
                }
            } catch (SocketTimeoutException e) {
                // timed out so will loop around again
            } catch (Throwable e) {
                // if the server socket has been closed then this is most likely
                // an unsubscribe so we don't try to report an error which would
                // just end up in RxJavaPlugins.onError as a stack trace in the
                // console.
                if (e instanceof SocketException && ("Socket closed".equals(e.getMessage())
                        || "Socket operation on nonsocket: configureBlocking"
                                .equals(e.getMessage()))) {
                    break;
                } else {
                    // unknown problem
                    emitter.onError(e);
                    break;
                }
            }
        }
    }

    @VisibleForTesting
    static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore exception
        }
    }

    private static Flowable<byte[]> createSocketFlowable(final Socket socket, long timeoutMs,
            final int bufferSize) {
        setTimeout(socket, timeoutMs);
        return Flowable.using( //
                new Callable<InputStream>() {
                    @Override
                    public InputStream call() throws Exception {
                        return socket.getInputStream();
                    }
                }, //
                new Function<InputStream, Flowable<byte[]>>() {
                    @Override
                    public Flowable<byte[]> apply(InputStream is) {
                        return Bytes.from(is, bufferSize);
                    }
                }, //
                Consumers.close(), //
                true);
    }

    private static void setTimeout(Socket socket, long timeoutMs) {
        try {
            socket.setSoTimeout((int) timeoutMs);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

}
