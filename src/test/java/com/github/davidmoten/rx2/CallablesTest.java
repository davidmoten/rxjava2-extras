package com.github.davidmoten.rx2;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class CallablesTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(Callables.class);
    }

}
