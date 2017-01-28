package com.github.davidmoten.rx2.util;

import org.junit.Assert;
import org.junit.Test;

public class PairTest {

    @Test
    public void testValues() {
        Pair<Integer, Integer> p = Pair.create(1, 2);
        Assert.assertEquals(1, (int) p.a());
        Assert.assertEquals(1, (int) p.left());
        Assert.assertEquals(1, (int) p._1());
        Assert.assertEquals(2, (int) p.b());
        Assert.assertEquals(2, (int) p.right());
        Assert.assertEquals(2, (int) p._2());
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(994, Pair.create(1, 2).hashCode());
    }

    @Test
    public void testHashCodeWithNulls() {
        Assert.assertEquals(961, Pair.create(null, null).hashCode());
    }

    @Test
    public void testToString() {
        Assert.assertEquals("Pair [left=1, right=2]", Pair.create(1, 2).toString());
    }
}
