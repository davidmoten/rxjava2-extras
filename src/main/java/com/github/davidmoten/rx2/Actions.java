package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Subscription;

import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;

public final class Actions {

    private Actions() {
        // prevent instantiation
    }

    public static Action setToTrue(final AtomicBoolean b) {
        return new Action() {

            @Override
            public void run() throws Exception {
                b.set(true);
            }

        };
    }

    public static Action throwing(final Exception e) {
        return new Action() {

            @Override
            public void run() throws Exception {
                throw e;
            }
            
        };
    }

    public static Action doNothing() {
        //TODO make holder
        return new Action() {
            @Override
            public void run() throws Exception {
                // do nothing!
            }
        };
    }



}
