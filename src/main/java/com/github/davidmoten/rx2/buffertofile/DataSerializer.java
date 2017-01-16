package com.github.davidmoten.rx2.buffertofile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface DataSerializer<T> {

    void serialize(T t, DataOutput out) throws IOException;

    T deserialize(DataInput in) throws IOException;

    /**
     * Returns 0 to indicate unknown (unbounded) capacity. Otherwise returns a
     * value that will be used to size internal byte arrays to receive the
     * serialized bytes ready for deserialization. An appropriate sizeHint will
     * reduce array copying (like in `ByteArrayOutputStream`) to improve
     * performance.
     * 
     * @return size hint to avoid byte array copying.
     */
    int sizeHint();
}
