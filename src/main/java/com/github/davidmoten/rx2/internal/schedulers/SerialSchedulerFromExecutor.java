package com.github.davidmoten.rx2.internal.schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.Scheduler;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.EmptyDisposable;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.plugins.RxJavaPlugins;

public final class SerialSchedulerFromExecutor extends Scheduler {

    private final ExecutorService executor;

    public SerialSchedulerFromExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public Worker createWorker() {
        return new SerialWorkerFromExecutor(executor);
    }

    private static final class SerialWorkerFromExecutor extends Worker {

        private final MpscLinkedQueue<Runnable> queue = new MpscLinkedQueue<Runnable>();
        private final AtomicBoolean executorInUse = new AtomicBoolean();
        private final ExecutorService executor;
        private volatile boolean disposed;

        SerialWorkerFromExecutor(ExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public Disposable schedule(Runnable run, long delay, TimeUnit unit) {
            if (disposed) {
                return EmptyDisposable.INSTANCE;
            }
            if (delay <= 0) {
                return scheduleNow(run);
            } else {
                // TODO implement
                throw new UnsupportedOperationException(
                        "scheduling tasks in the future is not supported by this scheduler yet");
            }
        }

        private Disposable scheduleNow(Runnable run) {
            DisposableNotifyingRunnable r = new DisposableNotifyingRunnable(run, this);
            queue.offer(r);
            Disposable d = drain();
            if (d != null) {
                return d;
            } else {
                return r;
            }
        }

        private Disposable drain() {
            if (executorInUse.compareAndSet(false, true)) {
                final Runnable run = queue.poll();
                if (run != null) {
                    try {
                        executor.execute(run);
                    } catch (RejectedExecutionException ex) {
                        disposed = true;
                        queue.clear();
                        RxJavaPlugins.onError(ex);
                        return EmptyDisposable.INSTANCE;
                    }
                }
            }
            return null;
        }

        // default visibilty to avoid extra method creation for access from another
        // class
        void taskFinished() {
            executorInUse.lazySet(false);
            // calling drain should not cause stack overflow because
            // all non-trivial executors break up the call chain
            // via trampolining or similar
            drain();
        }

        @Override
        public void dispose() {
            if (!disposed) {
                disposed = true;
                // TODO dispose tasks scheduled in future
                // tasks.dispose();
                // TODO concurrency protection like a WIP check?
                queue.clear();
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed;
        }

    }

    private static final class DisposableNotifyingRunnable extends AtomicBoolean implements Runnable, Disposable {

        private static final long serialVersionUID = -7359366974479666336L;

        private final Runnable runnable;

        private final SerialWorkerFromExecutor worker;

        DisposableNotifyingRunnable(Runnable runnable, SerialWorkerFromExecutor worker) {
            this.runnable = runnable;
            this.worker = worker;
        }

        @Override
        public void run() {
            if (get()) {
                return;
            }
            try {
                runnable.run();
            } finally {
                lazySet(true);
                worker.taskFinished();
            }
        }

        @Override
        public void dispose() {
            lazySet(true);
        }

        @Override
        public boolean isDisposed() {
            return get();
        }
    }

}
