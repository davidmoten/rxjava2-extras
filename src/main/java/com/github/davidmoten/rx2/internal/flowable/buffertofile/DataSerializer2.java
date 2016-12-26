package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.InputStream;
import java.io.OutputStream;

public interface DataSerializer2<T> {

    void serialize(T t, OutputStream os);
    
    T deserialize(InputStream is);
}
