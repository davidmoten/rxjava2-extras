package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.Transformers;
import com.github.davidmoten.rx2.internal.flowable.buffertofile.DataSerializer2Bytes;

import io.reactivex.Flowable;

public class FlowableOnBackpressureBufferToFileTest {

    @Test
    public void testJavaIOSerializable() {
        Flowable.just(1, 2, 3) //
                .compose(Transformers.<Integer>onBackpressureBufferToFile(1000000)) //
                .test() //
                .awaitDone(5000000000L, TimeUnit.SECONDS) //
                .assertValues(1, 2, 3)//
                .assertComplete();
    }

    @Test
    public void testByteArrays() {
        byte[] bytes = new byte[] {1,2,3};
        Flowable.just(bytes) //
                .compose(Transformers.<byte[]>onBackpressureBufferToFile(1000000, new DataSerializer2Bytes())) //
                .doOnNext(Consumers.assertBytesEquals(bytes)) //
                .doOnError(Consumers.printStackTrace()) //
                .test() //
                .awaitDone(5L, TimeUnit.SECONDS) //
                .assertComplete();
    }
    
    @Test
    public void testManyIntegers() {
        int n = 1000000;
        Flowable.range(1, n) //
        .compose(Transformers.<Integer>onBackpressureBufferToFile(1000000)) //
        .test() //
        .awaitDone(500L, TimeUnit.SECONDS) //
        .assertValueCount(n)//
        .assertComplete();
    }
}
