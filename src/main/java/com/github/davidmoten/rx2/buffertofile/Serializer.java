package com.github.davidmoten.rx2.buffertofile;

import java.io.IOException;

public interface Serializer<T> {

    /**
     * Returns a byte array of length &gt; 0 that is the serialization of the given
     * value. Note that there are performance advantages if you ensure that the
     * byte array produced has a length that is a multiple of four.
     * 
     * @param t
     *            value to be serialized into a byte array, should not be null.
     * @return a byte array of length &gt; 0
     * @throws IOException on error
     */
    byte[] serialize(T t) throws IOException;

    /**
     * Returns a non-null instance of T from the byte array of length &gt; 0.
     * 
     * @param bytes
     *            byte array, should have length &gt; 0
     * @return instance of T
     * @throws IOException on error
     * @throws ClassNotFoundException
     *             if class T not found
     */
    T deserialize(byte[] bytes) throws IOException, ClassNotFoundException;
}
