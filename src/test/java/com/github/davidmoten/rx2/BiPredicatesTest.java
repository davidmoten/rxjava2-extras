package com.github.davidmoten.rx2;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class BiPredicatesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(BiPredicates.class);
    }

    @Test
    public void testAlwaysTrue() throws Exception {
        Assert.assertTrue(BiPredicates.alwaysTrue().test(new Object(), new Object()));
    }

    @Test
    public void testAlwaysFalse() throws Exception {
        Assert.assertFalse(BiPredicates.alwaysFalse().test(new Object(), new Object()));
    }

}
