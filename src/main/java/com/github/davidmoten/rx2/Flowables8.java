package com.github.davidmoten.rx2;

import com.github.davidmoten.rx2.buffertofile.Options;

public final class Flowables8<T> {

    private Flowables8() {
        // prevent instantiation
    }

    public static Options.BuilderFlowable onBackpressureBufferToFile() {
        return Options.builderFlowable();
    }

}
