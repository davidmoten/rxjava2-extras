package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.github.davidmoten.rx2.buffertofile.Serializer;

public final class SerializerJavaIO<T extends Serializable> implements Serializer<T> {

    @Override
    public byte[] serialize(Serializable t) throws IOException {
        ByteArrayOutputStream bos= new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(t);
        bos.close();
        return bos.toByteArray();
    }

    @Override
    public T deserialize(byte[] bytes) throws ClassNotFoundException, IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bis);
            @SuppressWarnings("unchecked")
            T t = (T) ois.readObject();
            return t;
        } finally {
            if (ois != null) {
                ois.close();
            }
        }
    }

}
