package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class StringSearchLinkedListTest {

    @Test
    public void testEmpty() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        assertNull(s.next());
    }
    
    @Test
    public void testNotEmptyNotFound() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo");
        assertNull(s.next());
    }
    
    @Test
    public void testFindsFirst() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:and");
        assertEquals("boo",s.next());
        assertEquals(4, s.searchPosition());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsTwo() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:and:sue");
        assertEquals("boo",s.next());
        assertEquals("and",s.next());
        assertNull(s.next());
    }
    
}
