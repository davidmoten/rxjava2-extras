package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.buffertofile.Options;

public final class Observables8 {

    private Observables8() {
        // prevent instantiation
    }

    public static Options.BuilderObservable onBackpressureBufferToFile() {
        return Options.builderObservable();
    }
}
