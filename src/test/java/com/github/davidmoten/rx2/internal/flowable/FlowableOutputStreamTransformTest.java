package com.github.davidmoten.rx2.internal.flowable;

import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Test;

import com.github.davidmoten.rx2.flowable.Transformers;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

public class FlowableOutputStreamTransformTest {

    @Test
    public void test() {
        Flowable.just("hi there".getBytes())
                .compose(Transformers.outputStream(new Function<OutputStream, OutputStream>() {
                    @Override
                    public OutputStream apply(OutputStream os) throws Exception {
                        return new GZIPOutputStream(os);
                    }
                }, 8192, 16))
                .test();
    }

}
