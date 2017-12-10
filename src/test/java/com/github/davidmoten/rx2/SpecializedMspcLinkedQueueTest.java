package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class SpecializedMspcLinkedQueueTest {

    @Test
    public void testEmptyQueuePoll() {
        SpecializedMpscLinkedQueue<Integer> q = SpecializedMpscLinkedQueue.create();
        assertNull(q.poll());
        assertNull(q.poll());
    }

    @Test
    public void testPushPoll() {
        SpecializedMpscLinkedQueue<Integer> q = SpecializedMpscLinkedQueue.create();
        q.offer(1);
        assertEquals(1, (int) q.poll());
        assertNull(q.poll());
    }

    @Test
    public void testPushPushPollPoll() {
        SpecializedMpscLinkedQueue<Integer> q = SpecializedMpscLinkedQueue.create();
        q.offer(1);
        q.offer(2);
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) q.poll());
        assertNull(q.poll());
    }

    @Test
    public void testPushPushPollPushPollPoll() {
        SpecializedMpscLinkedQueue<Integer> q = SpecializedMpscLinkedQueue.create();
        q.offer(1);
        q.offer(2);
        assertEquals(1, (int) q.poll());
        q.offer(3);
        assertEquals(2, (int) q.poll());
        assertEquals(3, (int) q.poll());
        assertNull(q.poll());
    }

}
