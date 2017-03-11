package com.github.davidmoten.rx2.observable;

import io.reactivex.Observable;

public final class CloseableObservableWithReset<T> {

    private final Observable<T> observable;
    private final Runnable closeAction;
    private final Runnable resetAction;

    public CloseableObservableWithReset(Observable<T> observable, Runnable closeAction, Runnable resetAction) {
        this.observable = observable;
        this.closeAction = closeAction;
        this.resetAction = resetAction;
    }

    public Observable<T> observable() {
        return observable;
    }

    public void reset() {
        resetAction.run();
    }

    public void close() {
        closeAction.run();
    }

}