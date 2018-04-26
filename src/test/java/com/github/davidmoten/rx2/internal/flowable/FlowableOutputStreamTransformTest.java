package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;

import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public class FlowableOutputStreamTransformTest {

    @Test
    public void test() throws IOException {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Flowable.just("hi there".getBytes())
                .compose(Transformers.outputStream(new Function<OutputStream, OutputStream>() {
                    @Override
                    public OutputStream apply(OutputStream os) throws Exception {
                        return new GZIPOutputStream(os);
                    }
                }, 8192, 16)).doOnNext(new Consumer<byte[]>() {
                    @Override
                    public void accept(byte[] b) throws Exception {
                        bytes.write(b);
                    }
                }).test()
                .assertComplete();
        InputStream is = new GZIPInputStream(new ByteArrayInputStream(bytes.toByteArray()));
        
        assertEquals("hi there",read(is));
    }

    public static String read(InputStream input) throws IOException {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(input));
        return buffer.lines().collect(Collectors.joining("\n"));
    }
}
