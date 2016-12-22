package com.github.davidmoten.rx2;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
        PrintStream err = System.err;
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            System.setErr(new PrintStream(bytes));
            Consumers.printStackTrace().accept(new RuntimeException());
            String message = new String(bytes.toByteArray());
            assertTrue(message.startsWith("java.lang.RuntimeException"));
            assertTrue(message.contains("ConsumersTest.testPrintStackTrace"));
        } finally {
            System.setErr(err);
        }
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
