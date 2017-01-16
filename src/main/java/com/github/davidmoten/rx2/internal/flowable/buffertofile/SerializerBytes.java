package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.IOException;

import com.github.davidmoten.rx2.buffertofile.Serializer;

public final class SerializerBytes implements Serializer<byte[]> {

    @Override
    public byte[] serialize(byte[] t) throws IOException {
        return t;
    }

    @Override
    public byte[] deserialize(byte[] bytes) throws ClassNotFoundException, IOException {
        return bytes;
    }

}
