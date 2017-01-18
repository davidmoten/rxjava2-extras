package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.Callable;

import org.junit.Test;

public class PageListTest {

    private static final Callable<File> factory = new Callable<File>() {
        int i = 0;

        @Override
        public File call() throws Exception {
            return new File("target/test" + (++i));
        }
    };

    @Test
    public void testPutOneAndReadLessThanPageSize() {
        Pages p = createPages(8);
        p.putInt(1);
        assertEquals(1, p.getInt());
    }
    
    private static Pages createPages(int size) {
        return new Pages(factory, size, false);
    }

    @Test
    public void testPutTwoAndReadLessThanPageSize() {
        Pages p = createPages(12);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getInt());
        assertEquals(2, p.getInt());
    }

    @Test
    public void testPutTwoAndReadEqualsPageSize() {
        Pages p = createPages(8);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getInt());
        assertEquals(2, p.getInt());
    }

    @Test
    public void testPutTwoAndReadMoreThanPageSize() {
        Pages p = createPages(4);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getInt());
        assertEquals(2, p.getInt());
    }

    @Test
    public void testPutSixAndReadMultiplePages() {
        Pages p = createPages(8);
        p.putInt(1);
        p.putInt(2);
        p.putInt(3);
        p.putInt(4);
        p.putInt(5);
        p.putInt(6);
        assertEquals(1, p.getInt());
        assertEquals(2, p.getInt());
        assertEquals(3, p.getInt());
        assertEquals(4, p.getInt());
        assertEquals(5, p.getInt());
        assertEquals(6, p.getInt());
    }

    // TODO test rewrite marking
}
