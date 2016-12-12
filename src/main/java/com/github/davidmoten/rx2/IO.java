package com.github.davidmoten.rx2;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;

import com.github.davidmoten.rx2.internal.flowable.FlowableServerSocket;

import io.reactivex.Flowable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.internal.functions.Functions;

public final class IO {

    private IO() {
        // prevent instantiation
    }

    public static ServerSocketBuilder serverSocket(final int port) {
        return new ServerSocketBuilder(new Callable<ServerSocket>() {
            @Override
            public ServerSocket call() throws IOException {
                return new ServerSocket(port);
            }
        });
    }

    public static ServerSocketBuilder serverSocketAutoAllocatePort(
            final Consumer<Integer> onAllocated) {
        return serverSocket(new Callable<ServerSocket>() {

            @Override
            public ServerSocket call() throws Exception {
                    ServerSocket ss = new ServerSocket(0);
                    onAllocated.accept(ss.getLocalPort());
                    return ss;
            }
        });
    }

    public static ServerSocketBuilder serverSocket(
            Callable<? extends ServerSocket> serverSocketFactory) {
        return new ServerSocketBuilder(serverSocketFactory);
    }

    public static final class ServerSocketBuilder {

        private final Callable<? extends ServerSocket> serverSocketFactory;
        private int readTimeoutMs = Integer.MAX_VALUE;
        private int bufferSize = 8192;
        private Action preAcceptAction = Actions.doNothing();
        private int acceptTimeoutMs = Integer.MAX_VALUE;
        private Predicate<? super Socket> acceptSocket = Functions.<Socket>alwaysTrue();

        public ServerSocketBuilder(final Callable<? extends ServerSocket> serverSocketFactory) {
            this.serverSocketFactory = serverSocketFactory;
        }

        public ServerSocketBuilder readTimeoutMs(int readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
            return this;
        }

        public ServerSocketBuilder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public ServerSocketBuilder preAcceptAction(Action action) {
            this.preAcceptAction = action;
            return this;
        }

        public ServerSocketBuilder acceptTimeoutMs(int acceptTimeoutMs) {
            this.acceptTimeoutMs = acceptTimeoutMs;
            return this;
        }

        public ServerSocketBuilder acceptSocketIf(Predicate<? super Socket> acceptSocket) {
            this.acceptSocket = acceptSocket;
            return this;
        }

        public Flowable<Flowable<byte[]>> create() {
            return FlowableServerSocket.create(serverSocketFactory, readTimeoutMs, bufferSize,
                    preAcceptAction, acceptTimeoutMs, acceptSocket);
        }

    }

}