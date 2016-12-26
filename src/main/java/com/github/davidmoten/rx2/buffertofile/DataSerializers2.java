package com.github.davidmoten.rx2.buffertofile;

import java.io.Serializable;

import com.github.davidmoten.rx2.internal.flowable.buffertofile.DataSerializer2JavaIO;

public class DataSerializers2 {

    public static <T extends Serializable> DataSerializer2<T> javaIO() {
        return new DataSerializer2JavaIO<T>();
    }

}
