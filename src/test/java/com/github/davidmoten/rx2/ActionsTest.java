package com.github.davidmoten.rx2;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class ActionsTest {

    @Test
    public void testSetToTrue() throws Exception {
        AtomicBoolean b = new AtomicBoolean();
        Actions.setToTrue(b).run();
        assertTrue(b.get());
    }

    @Test
    public void testIsUtility() {
        Asserts.assertIsUtilityClass(Actions.class);
    }

}
