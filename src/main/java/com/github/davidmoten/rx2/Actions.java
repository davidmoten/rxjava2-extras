package com.github.davidmoten.rx2;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.IllegalBlockSizeException;

import io.reactivex.functions.Action;

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

}
