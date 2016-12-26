package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.davidmoten.rx2.buffertofile.DataSerializer2;

public class DataSerializer2Bytes implements DataSerializer2<byte[]> {

    @Override
    public void serialize(byte[] t, OutputStream os) throws IOException {
        os.write(t);
    }

    @Override
    public byte[] deserialize(InputStream is, int length) throws ClassNotFoundException, IOException {
        //TODO don't want copy!
        byte[] bytes = new byte[length];
        is.read(bytes);
        return bytes;
    }

}
