package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.buffertofile.Options;

public final class ObservableTransformers {

    private ObservableTransformers() {
        // prevent instantiation
    }

    public static Options.BuilderObservable onBackpressureBufferToFile() {
        return Options.builderObservable();
    }
}
