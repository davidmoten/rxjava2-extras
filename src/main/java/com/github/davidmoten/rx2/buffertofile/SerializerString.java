package com.github.davidmoten.rx2.buffertofile;

import java.io.IOException;
import java.nio.charset.Charset;

public final class SerializerString implements Serializer<String> {

    private final Charset charset;

    public SerializerString(Charset charset) {
        this.charset = charset;
    }

    @Override
    public byte[] serialize(String s) throws IOException {
        return s.getBytes(charset);
    }

    @Override
    public String deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        return new String(bytes, charset);
    }

}
