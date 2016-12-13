package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class FunctionsTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(Functions.class);
    }

    @Test
    public void testConstant() throws Exception {
        assertEquals("boo", Functions.constant("boo").apply("blah"));
    }

}
