package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;

import io.reactivex.Flowable;

public class FlowableMinRequestTest {

    @Test
    public void testConstrainedFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, true)) //
                .rebatchRequests(1) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(2L, 2L, 2L, 2L, 2L, 2L), requests);
    }

    @Test
    public void testUnconstrainedFirstRequest() {
        List<Long> requests = new CopyOnWriteArrayList<Long>();
        Flowable.range(1, 10) //
                .doOnRequest(Consumers.addLongTo(requests)) //
                .compose(FlowableTransformers.minRequest(2, false)) //
                .rebatchRequests(1) //
                .test() //
                .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) //
                .assertComplete();
        assertEquals(Lists.newArrayList(1L, 2L, 2L, 2L, 2L, 2L), requests);
    }

}
