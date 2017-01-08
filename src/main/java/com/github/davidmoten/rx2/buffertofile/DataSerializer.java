package com.github.davidmoten.rx2.buffertofile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface DataSerializer<T> {

    void serialize(T t, DataOutput out) throws IOException;

    T deserialize(DataInput in) throws IOException;

    /**
     * Returns 0 to indicate unknown (unbounded) capacity.
     * 
     * @return capacity hint to avoid byte array copying.
     */
    int capacity();
}
