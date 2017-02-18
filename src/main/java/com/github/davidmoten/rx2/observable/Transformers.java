package com.github.davidmoten.rx2.observable;

import com.github.davidmoten.rx2.buffertofile.Options;

public final class Transformers {

    private Transformers() {
        // prevent instantiation
    }

    public static Options.BuilderObservable onBackpressureBufferToFile() {
        return Options.builderObservable();
    }
}
