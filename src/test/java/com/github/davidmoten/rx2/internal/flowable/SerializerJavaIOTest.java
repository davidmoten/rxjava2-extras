package com.github.davidmoten.rx2.internal.flowable;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import com.github.davidmoten.rx2.buffertofile.Serializers;

public class SerializerJavaIOTest {

    @Test(expected = IOException.class)
    public void testNoHeader() throws IOException, ClassNotFoundException {
        Serializers.javaIO().deserialize(new byte[] {});
    }
    
    @Test(expected = IOException.class)
    public void testProblemAfterHeader() throws IOException, ClassNotFoundException {
        byte[] bytes = Serializers.javaIO().serialize(10);
        bytes = Arrays.copyOf(bytes, bytes.length-1);
        Serializers.javaIO().deserialize(bytes);
    }


}
