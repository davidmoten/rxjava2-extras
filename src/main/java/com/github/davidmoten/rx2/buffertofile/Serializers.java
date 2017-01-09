package com.github.davidmoten.rx2.buffertofile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

import com.github.davidmoten.rx2.internal.flowable.buffertofile.SerializerBytes;
import com.github.davidmoten.rx2.internal.flowable.buffertofile.SerializerJavaIO;

public final class Serializers {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private Serializers() {
        // prevent initialization
    }

    public static <T extends Serializable> Serializer<T> javaIO() {
        // TODO use holder
        return new SerializerJavaIO<T>();
    }

    public static Serializer<byte[]> bytes() {
        // TODO use holder
        return new SerializerBytes();
    }

    public static Serializer<String> utf8() {
        //TODO use holder 
        return string(UTF_8);
    }

    public static Serializer<String> string(Charset charset) {
        return new SerializerString(charset);
    }
    
    public static <T> Serializer<T> from(DataSerializer<T> ds){
        return new WrappedDataSerializer<T>(ds);
    }
    
    private static final class WrappedDataSerializer<T> implements Serializer<T> {

        private final DataSerializer<T> ds;

        WrappedDataSerializer(DataSerializer<T> ds) {
            this.ds = ds;
        }

        @Override
        public byte[] serialize(T t) throws IOException {
            ByteArrayOutputStream bytes;
            int cap = ds.sizeHint();
            if (cap > 0)
                bytes = new ByteArrayOutputStream(ds.sizeHint());
            else
                bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            ds.serialize(t, out);
            out.close();
            return bytes.toByteArray();
        }

        @Override
        public T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
            ByteArrayInputStream is = new ByteArrayInputStream(bytes);
            DataInputStream in = new DataInputStream(is);
            T t = ds.deserialize(in);
            in.close();
            return t;
        }

    }
    
    
}
