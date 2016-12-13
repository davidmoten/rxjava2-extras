package com.github.davidmoten.rx2;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class ConsumersTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(Consumers.class);
    }

    @Test
    public void testPrintStackTrace() throws Exception {
        Consumers.printStackTrace().accept(new RuntimeException());
    }

    @Test
    public void testDoNothing() throws Exception {
        Consumers.doNothing().accept(new Object());
    }

    @Test
    public void testSetToTrue() throws Exception {
        AtomicBoolean b = new AtomicBoolean();
        Consumers.setToTrue(b).accept(new Object());
        assertTrue(b.get());
    }

}
