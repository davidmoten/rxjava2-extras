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
    public void checkSingleMaxRequestOneDownstreamRequestsMaxValue() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.just(1) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.maxRequest(1)) //
                .test() //
                .assertValue(1) //
                .assertComplete();
        assertEquals(Arrays.asList(1L, 1L), requests);
    }

    @Test
    public void checkSingleMaxRequestTwoDownstreamRequestsMaxValue() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.just(1) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.maxRequest(2)) //
                .test() //
                .assertValue(1) //
                .assertComplete();
        assertEquals(Arrays.asList(2L), requests);
    }
    
    @Test
    public void checkSingleMaxRequestMaxValueDownstreamRequestsMaxValue() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.just(1) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.maxRequest(Long.MAX_VALUE)) //
                .test() //
                .assertValue(1) //
                .assertComplete();
        assertEquals(Arrays.asList(Long.MAX_VALUE), requests);
    }


    @Test
    public void checkMaxRequestOneDownstreamRequestMaxValue() {
        checkMaxRequestDownstreamRequestMaxValue(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L);
    }

    @Test
    public void checkMaxRequestTwoDownstreamRequestMaxValue() {
        checkMaxRequestDownstreamRequestMaxValue(2L, 2L, 2L, 2L, 2L, 2L, 2L);
    }
    
    @Test
    public void checkMaxRequestThreeDownstreamRequestMaxValue() {
        checkMaxRequestDownstreamRequestMaxValue(3L, 3L, 3L, 3L, 3L);
    }
    
    @Test
    public void checkMaxRequestOneDownstreamRequestTen() {
        checkMaxRequestDownstreamRequestTen(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L);
    }

    @Test
    public void checkMaxRequestTwoDownstreamRequestTen() {
        checkMaxRequestDownstreamRequestTen(2L, 2L, 2L, 2L, 2L, 2L);
    }
    
    @Test
    public void checkMaxRequestThreeDownstreamRequestTen() {
        checkMaxRequestDownstreamRequestTen(3L, 3L, 3L, 3L, 1L);
    }


    private void checkMaxRequestDownstreamRequestMaxValue(long maxRequest, Long... expectedRequests) {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.maxRequest(maxRequest)) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Arrays.asList(expectedRequests), requests);
    }
    
    private void checkMaxRequestDownstreamRequestTen(long maxRequest, Long... expectedRequests) {
        List<Long> list = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(list)) //
                .compose(FlowableTransformers.maxRequest(maxRequest)) //
                .test(10) //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Arrays.asList(expectedRequests), list);
    }

}
