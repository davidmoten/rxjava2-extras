package com.github.davidmoten.rx2.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.Functions;
import com.github.davidmoten.rx2.Statistics;
import com.github.davidmoten.rx2.util.Pair;

import io.reactivex.Flowable;

public class TransformersTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(Transformers.class);
    }

    @Test
    public void testStatisticsOnEmptyStream() {
        Flowable<Integer> nums = Flowable.empty();
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(0, s.count());
        assertEquals(0, s.sum(), 0.0001);
        assertTrue(Double.isNaN(s.mean()));
        assertTrue(Double.isNaN(s.sd()));
    }

    @Test
    public void testStatisticsOnSingleElement() {
        Flowable<Integer> nums = Flowable.just(1);
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(1, s.count());
        assertEquals(1, s.sum(), 0.0001);
        assertEquals(1.0, s.mean(), 0.00001);
        assertEquals(0, s.sd(), 0.00001);
    }

    @Test
    public void testStatisticsOnMultipleElements() {
        Flowable<Integer> nums = Flowable.just(1, 4, 10, 20);
        Statistics s = nums.compose(Transformers.<Integer>collectStats()).blockingLast();
        assertEquals(4, s.count());
        assertEquals(35.0, s.sum(), 0.0001);
        assertEquals(8.75, s.mean(), 0.00001);
        assertEquals(7.258615570478987, s.sd(), 0.00001);
        assertEquals(1+16+100+400, s.sumSquares(), 0.0001);
    }

    @Test
    public void testStatisticsPairOnEmptyStream() {
        Flowable<Integer> nums = Flowable.empty();
        boolean isEmpty = nums.compose(Transformers.collectStats(Functions.<Integer>identity())).isEmpty()
                .blockingGet();
        assertTrue(isEmpty);
    }

    @Test
    public void testStatisticsPairOnSingleElement() {
        Flowable<Integer> nums = Flowable.just(1);
        Pair<Integer, Statistics> s = nums.compose(Transformers.collectStats(Functions.<Integer>identity()))
                .blockingLast();
        assertEquals(1, (int) s.a());
        assertEquals(1, s.b().count());
        assertEquals(1, s.b().sum(), 0.0001);
        assertEquals(1.0, s.b().mean(), 0.00001);
        assertEquals(0, s.b().sd(), 0.00001);
    }

    @Test
    public void testStatisticsPairOnMultipleElements() {
        Flowable<Integer> nums = Flowable.just(1, 4, 10, 20);
        Pair<Integer, Statistics> s = nums.compose(Transformers.collectStats(Functions.<Integer>identity()))
                .blockingLast();
        assertEquals(4, s.b().count());
        assertEquals(35.0, s.b().sum(), 0.0001);
        assertEquals(8.75, s.b().mean(), 0.00001);
        assertEquals(7.258615570478987, s.b().sd(), 0.00001);
    }
    
    @Test
    public void testToString() {
        Statistics s = Statistics.create();
        s = s.add(1).add(2);
        assertEquals("Statistics [count=2, sum=3.0, sumSquares=5.0, mean=1.5, sd=0.5]", s.toString());
    }

}
