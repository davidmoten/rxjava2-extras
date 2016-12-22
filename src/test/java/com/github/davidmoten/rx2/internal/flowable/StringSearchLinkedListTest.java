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
    public void testFindsFirstLongDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList("::");
        s.add("boo::and");
        assertEquals("boo",s.next());
        assertEquals(5, s.searchPosition());
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
    
    @Test
    public void testFindsTwoLongDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList("::");
        s.add("boo::and::sue");
        assertEquals("boo",s.next());
        assertEquals("and",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsThree() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:and:sue:me");
        assertEquals("boo",s.next());
        assertEquals("and",s.next());
        assertEquals("sue",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterStartsSecond() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo");
        s.add(":and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterEndsFirst() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:");
        s.add("and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterEndsFirstLongDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList("::");
        s.add("boo::");
        s.add("and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterSplitAcrossTwo() {
        StringSearchLinkedList s = new StringSearchLinkedList("::");
        s.add("boo:");
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneEndsWithDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoEndsWithDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo");
        s.add(":");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsNoneBecauseOnlyPartialMatchToDelimiter() {
        StringSearchLinkedList s = new StringSearchLinkedList("::");
        s.add("boo:");
        s.add("and");
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterMiddleFirst() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("boo:a");
        s.add("nd");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossTwoDelimiterMiddleSecond() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("bo");
        s.add("o:and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossThree() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("bo");
        s.add("o");
        s.add(":and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
    @Test
    public void testFindsOneAcrossFour() {
        StringSearchLinkedList s = new StringSearchLinkedList(":");
        s.add("bo");
        s.add("o");
        s.add(":");
        s.add("and");
        assertEquals("boo",s.next());
        assertNull(s.next());
    }
    
}
