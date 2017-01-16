package com.github.davidmoten.rx2;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class PredicatesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Predicates.class);
    }
    
    @Test
    public void testAlwaysFalse() throws Exception {
        Assert.assertFalse(Predicates.alwaysFalse().test(new Object()));
    }
    
    
    @Test
    public void testAlwaysTrue() throws Exception {
        Assert.assertTrue(Predicates.alwaysTrue().test(new Object()));
    }
}
