package com.github.davidmoten.rx2.buffertofile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface DataSerializer2<T> {

    void serialize(T t, OutputStream os) throws IOException;
    
    T deserialize(InputStream is, int length) throws IOException, ClassNotFoundException;
}
