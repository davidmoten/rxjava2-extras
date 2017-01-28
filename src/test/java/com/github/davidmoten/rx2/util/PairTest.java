package com.github.davidmoten.rx2.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    @Test
    public void testEquals() {
        Pair<Integer, Integer> a = Pair.create(1, 2);
        Pair<Integer, Integer> b = Pair.create(1, 2);
        Pair<Integer, Integer> c = Pair.create(1, 3);
        Pair<Integer, Integer> d = Pair.create(3, 2);
        Pair<Integer, Integer> e = Pair.create(3, 4);
        Pair<Integer, Integer> f = Pair.create(1, null);
        Pair<Integer, Integer> g = Pair.create(null, 2);
        Pair<Integer, Integer> h = Pair.create(null, null);
        Pair<Integer, Integer> i = Pair.create(null, null);
        assertTrue(a.equals(b));
        assertTrue(a.equals(a));
        assertFalse(a.equals(c));
        assertFalse(a.equals(d));
        assertFalse(a.equals(e));
        assertFalse(a.equals(null));
        assertFalse(a.equals(f));
        assertFalse(a.equals(g));
        assertFalse(a.equals(1));
        assertTrue(f.equals(f));
        assertTrue(g.equals(g));
        assertFalse(f.equals(a));
        assertFalse(g.equals(a));
        assertTrue(h.equals(i));
    }
}
