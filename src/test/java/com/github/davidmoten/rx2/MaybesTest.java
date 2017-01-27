package com.github.davidmoten.rx2;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public final class MaybesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Maybes.class);
    }

    @Test
    public void testNull() {
        Assert.assertEquals(0, (long) Maybes.<Integer>fromNullable(null).count().blockingGet());
    }
    
    @Test
    public void testNotNull() {
        Assert.assertEquals(1, (long) Maybes.<Integer>fromNullable(1000).count().blockingGet());
    }

}
