package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import com.github.davidmoten.rx2.buffertofile.DataSerializer2;

public class DataSerializer2JavaIO<T extends Serializable> implements DataSerializer2<T> {

    @Override
    public void serialize(Serializable t, OutputStream os) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(os);
        oos.writeObject(t);
        oos.close();
    }

    @Override
    public T deserialize(InputStream is) throws ClassNotFoundException, IOException {
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(is);
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
