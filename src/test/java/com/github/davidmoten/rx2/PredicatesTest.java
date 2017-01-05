package com.github.davidmoten.rx2;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class PredicatesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Predicates.class);
    }
    
}
