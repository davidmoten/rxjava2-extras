package com.github.davidmoten.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

public class RingBufferTest {

    @Test
    public void testEmpty() {
        RingBuffer<Integer> q = RingBuffer.create(3);
        assertTrue(q.isEmpty());
        assertEquals(0, q.size());
    }

    @Test
    public void testPush() {
        RingBuffer<Integer> q = RingBuffer.create(3);
        q.add(1);
        assertFalse(q.isEmpty());
        assertEquals(1, q.size());
        assertEquals(1, (int) q.poll());
        assertTrue(q.isEmpty());
    }

    @Test
    public void testPushTwo() {
        RingBuffer<Integer> q = RingBuffer.create(3);
        q.add(1);
        q.add(2);
        assertFalse(q.isEmpty());
        assertEquals(2, q.size());
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) q.poll());
        assertTrue(q.isEmpty());
        q.add(3);
        q.add(4);
        assertEquals(3, (int) q.poll());
        assertEquals(4, (int) q.poll());
    }

    @Test(expected = RuntimeException.class)
    public void testPushThreeOverflows() {
        RingBuffer<Integer> q = RingBuffer.create(2);
        q.add(1);
        q.add(2);
        q.add(3);
        assertFalse(q.isEmpty());
        assertEquals(3, q.size());
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) q.poll());
        assertEquals(3, (int) q.poll());
    }

    public void testPushThreeInSizeThree() {
        RingBuffer<Integer> q = RingBuffer.create(3);
        q.add(1);
        q.add(2);
        q.add(3);
        assertFalse(q.isEmpty());
        assertEquals(3, q.size());
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) q.poll());
        assertEquals(3, (int) q.poll());
    }

    public void testPushThreeAndEnumerate() {
        RingBuffer<Integer> q = RingBuffer.create(3);
        q.add(1);
        q.add(2);
        q.add(3);
        Iterator<Integer> en = q.iterator();
        assertEquals(1, (int) en.next());
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) en.next());
        assertEquals(2, (int) q.poll());
        assertEquals(3, (int) en.next());
        assertEquals(3, (int) q.poll());
        assertFalse(en.hasNext());
    }
}