package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class MspcQueueTest {

    @Test
    public void testEmptyQueuePoll() {
        MpscQueue<Integer> q = new MpscQueue<Integer>();
        assertNull(q.poll());
        assertNull(q.poll());
    }

    @Test
    public void testPushPoll() {
        MpscQueue<Integer> q = new MpscQueue<Integer>();
        q.offer(1);
        assertEquals(1, (int) q.poll());
        assertNull(q.poll());
    }
    
    @Test
    public void testPushPushPollPoll() {
        MpscQueue<Integer> q = new MpscQueue<Integer>();
        q.offer(1);
        q.offer(2);
        assertEquals(1, (int) q.poll());
        assertEquals(2, (int) q.poll());
        assertNull(q.poll());
    }

}
