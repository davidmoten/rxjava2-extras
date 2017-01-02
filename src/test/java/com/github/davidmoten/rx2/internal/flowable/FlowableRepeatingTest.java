package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

import com.github.davidmoten.rx2.Flowables;

public class FlowableRepeatingTest {

    @Test
    public void testRepeatingTwo() {
        assertEquals(Arrays.asList(1000, 1000),
                Flowables.repeat(1000).take(2).toList().blockingGet());
    }

    @Test
    public void testRepeatingZero() {
        Flowables.repeat(1000) //
                .test(0) //
                .assertNoValues() //
                .assertNotComplete() //
                .requestMore(1) //
                .assertValue(1000) //
                .assertNotComplete();
    }

    @Test
    public void testRepeatingWithCount() {
        Flowables.repeat(1, 3) //
                .test() //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

}
