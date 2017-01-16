package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class UnsafeAccessTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(UnsafeAccess.class);
    }
}
