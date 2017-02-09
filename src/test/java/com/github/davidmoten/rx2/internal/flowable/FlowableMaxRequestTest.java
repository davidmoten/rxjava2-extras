package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;

import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;

import io.reactivex.Flowable;

public class FlowableMaxRequestTest {

    @Test
    public void test() {
        checkMaxRequest(1);
    }
    
    @Test
    public void test1() {
        checkMaxRequest(2);
    }
    
    @Test
    public void testMaxValue() {
        checkMaxRequest(Long.MAX_VALUE);
    }

    private void checkMaxRequest(long maxRequest) {
        List<Long> list = new CopyOnWriteArrayList<Long>();
        Flowable.just(1) //
                .doOnRequest(Consumers.addLongTo(list)) //
                .compose(FlowableTransformers.maxRequest(maxRequest)) //
                .test() //
                .assertValue(1) //
                .assertComplete();
        assertEquals(Arrays.asList(maxRequest), list);
    }

}
