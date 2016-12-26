package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.davidmoten.rx2.Transformers;

import io.reactivex.Flowable;

public class FlowableOnBackpressureBufferToFileTest {

    @Test
    public void test() {
        Flowable.just(1, 2, 3) //
                .compose(Transformers.<Integer>onBackpressureBufferToFile(1000000)) //
                .test() //
                .awaitDone(5, TimeUnit.SECONDS) //
                .assertValues(1, 2, 3)//
                .assertComplete();
    }
}
