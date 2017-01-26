package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class DelimitedStringLinkedListTest {

    @Test
    public void testEmpty() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        assertNull(s.next());
    }

    @Test
    public void testNotEmptyNotFound() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo");
        assertNull(s.next());
        assertEquals("boo", s.remaining());
    }

    @Test
    public void testFindsFirst() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:and");
        assertEquals("boo", s.next());
        assertEquals(4, s.searchPosition());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsFirstLongDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList("::");
        s.add("boo::and");
        assertEquals("boo", s.next());
        assertEquals(5, s.searchPosition());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsTwo() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:and:sue");
        assertEquals("boo", s.next());
        assertEquals("and", s.next());
        assertNull(s.next());
        assertEquals("sue", s.remaining());
    }

    @Test
    public void testFindsTwoLongDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList("::");
        s.add("boo::and::sue");
        assertEquals("boo", s.next());
        assertEquals("and", s.next());
        assertNull(s.next());
        assertEquals("sue", s.remaining());
    }

    @Test
    public void testFindsThree() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:and:sue:me");
        assertEquals("boo", s.next());
        assertEquals("and", s.next());
        assertEquals("sue", s.next());
        assertNull(s.next());
        assertEquals("me", s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterStartsSecond() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo");
        s.add(":and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterEndsFirst() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:");
        s.add("and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterEndsFirstLongDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList("::");
        s.add("boo::");
        s.add("and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterSplitAcrossTwo() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList("::");
        s.add("boo:");
        assertNull(s.next());
        assertEquals("boo:", s.remaining());
    }

    @Test
    public void testFindsOneEndsWithDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertNull(s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoEndsWithDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo");
        s.add(":");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertNull(s.remaining());
    }

    @Test
    public void testFindsNoneBecauseOnlyPartialMatchToDelimiter() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList("::");
        s.add("boo:");
        s.add("and");
        assertNull(s.next());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterMiddleFirst() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("boo:a");
        s.add("nd");
        assertEquals("boo", s.next());
        assertEquals(4, s.searchPosition());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossTwoDelimiterMiddleSecond() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("bo");
        s.add("o:and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossThree() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("bo");
        s.add("o");
        s.add(":and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

    @Test
    public void testFindsOneAcrossFour() {
        DelimitedStringLinkedList s = new DelimitedStringLinkedList(":");
        s.add("bo");
        s.add("o");
        s.add(":");
        s.add("and");
        assertEquals("boo", s.next());
        assertNull(s.next());
        assertEquals("and", s.remaining());
    }

}
