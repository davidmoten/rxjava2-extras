package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.Callable;

import org.junit.Assert;
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
        PageList p = new PageList(factory, 8);
        p.put(1);
        assertEquals(1, p.read());
    }

    @Test
    public void testPutTwoAndReadLessThanPageSize() {
        PageList p = new PageList(factory, 12);
        p.put(1);
        p.put(2);
        assertEquals(1, p.read());
        assertEquals(2, p.read());
    }
    
    @Test
    public void testPutTwoAndReadEqualsPageSize() {
        PageList p = new PageList(factory, 8);
        p.put(1);
        p.put(2);
        assertEquals(1, p.read());
        assertEquals(2, p.read());
    }
    
    @Test
    public void testPutTwoAndReadMoreThanPageSize() {
        PageList p = new PageList(factory, 5);
        p.put(1);
        p.put(2);
        assertEquals(1, p.read());
        assertEquals(2, p.read());
    }

}
