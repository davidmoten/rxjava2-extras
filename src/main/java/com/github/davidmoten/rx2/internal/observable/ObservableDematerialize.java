package com.github.davidmoten.rx2.internal.observable;

import io.reactivex.Notification;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class ObservableDematerialize<T> extends Observable<T> {

    private final ObservableSource<Notification<T>> source;

    public ObservableDematerialize(ObservableSource<Notification<T>> source) {
        this.source = source;
    }

    @Override
    public void subscribeActual(Observer<? super T> t) {
        source.subscribe(new DematerializeObserver<T>(t));
    }

    static final class DematerializeObserver<T> implements Observer<Notification<T>>, Disposable {
        final Observer<? super T> actual;

        boolean done;

        Disposable s;

        DematerializeObserver(Observer<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Disposable s) {
            if (DisposableHelper.validate(this.s, s)) {
                this.s = s;

                actual.onSubscribe(this);
            }
        }

        @Override
        public void dispose() {
            s.dispose();
        }

        @Override
        public boolean isDisposed() {
            return s.isDisposed();
        }

        @Override
        public void onNext(Notification<T> t) {
            if (done) {
                if (t.isOnError()) {
                    RxJavaPlugins.onError(t.getError());
                }
                return;
            }
            if (t.isOnError()) {
                s.dispose();
                onError(t.getError());
            } else if (t.isOnComplete()) {
                s.dispose();
                done = true;
                actual.onComplete();
            } else {
                actual.onNext(t.getValue());
            }
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            done = true;

            actual.onError(t);
        }

        @Override
        public void onComplete() {
            // ignore
        }
    }
}
