package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        assertEquals(1, p.get());
    }

    @Test
    public void testPutTwoAndReadLessThanPageSize() {
        PageList p = new PageList(factory, 12);
        p.put(1);
        p.put(2);
        assertEquals(1, p.get());
        assertEquals(2, p.get());
    }

    @Test
    public void testPutTwoAndReadEqualsPageSize() {
        PageList p = new PageList(factory, 8);
        p.put(1);
        p.put(2);
        assertEquals(1, p.get());
        assertEquals(2, p.get());
    }

    @Test
    public void testPutTwoAndReadMoreThanPageSize() {
        PageList p = new PageList(factory, 5);
        p.put(1);
        p.put(2);
        assertEquals(1, p.get());
        assertEquals(2, p.get());
    }

    @Test
    public void testPutSixAndReadMultiplePages() {
        PageList p = new PageList(factory, 5);
        p.put(1);
        p.put(2);
        p.put(3);
        p.put(4);
        p.put(5);
        p.put(6);
        assertEquals(1, p.get());
        assertEquals(2, p.get());
        assertEquals(3, p.get());
        assertEquals(4, p.get());
        assertEquals(5, p.get());
        assertEquals(6, p.get());
    }

    @Test
    public void testMarkAtBeginning() {
        PageList p = new PageList(factory, 16);
        Assert.assertFalse(p.marked);
        p.mark();
        assertTrue(p.marked);
        p.put(1);
        assertEquals(4, p.writePosition);
        assertEquals(4, p.currentWritePosition);
        p.moveToMark();

        p.put(2);
        p.put(3);
        assertEquals(2, p.get());
        assertEquals(3, p.get());
    }

    @Test
    public void testMoveToEnd() {
        PageList p = new PageList(factory, 16);
        p.mark();
        p.put(1);
        p.moveToMark();
        p.put(2);
        p.moveToEnd();
        p.clearMark();
        p.put(3);
        assertEquals(2, p.get());
        assertEquals(3, p.get());
    }

    @Test
    public void testMarkInMiddle() {
        PageList p = new PageList(factory, 8);
        p.put(1);
        p.mark();
        p.put(2);
        p.put(3);
        p.moveToMark();
        p.put(4);
        p.clearMark();
        p.moveToEnd();
        p.put(5);
        assertEquals(1, p.get());
    }
}
