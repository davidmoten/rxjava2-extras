package com.github.davidmoten.rx2.internal.flowable;

import io.reactivex.Flowable;

public final class CloseableFlowableWithReset<T> {

    private final Flowable<T> flowable;
    private final Runnable closeAction;
    private final Runnable resetAction;

    public CloseableFlowableWithReset(Flowable<T> flowable, Runnable closeAction, Runnable resetAction) {
        this.flowable = flowable;
        this.closeAction = closeAction;
        this.resetAction = resetAction;
    }

    public Flowable<T> flowable() {
        return flowable;
    }

    public void reset() {
        resetAction.run();
    }

    public void close() {
        closeAction.run();
    }

}