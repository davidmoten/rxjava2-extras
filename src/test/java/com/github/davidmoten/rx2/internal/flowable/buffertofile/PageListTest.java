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
        p.putInt(1);
        assertEquals(1, p.getPositiveInt());
    }

    @Test
    public void testPutTwoAndReadLessThanPageSize() {
        PageList p = new PageList(factory, 12);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getPositiveInt());
        assertEquals(2, p.getPositiveInt());
    }

    @Test
    public void testPutTwoAndReadEqualsPageSize() {
        PageList p = new PageList(factory, 8);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getPositiveInt());
        assertEquals(2, p.getPositiveInt());
    }

    @Test
    public void testPutTwoAndReadMoreThanPageSize() {
        PageList p = new PageList(factory, 5);
        p.putInt(1);
        p.putInt(2);
        assertEquals(1, p.getPositiveInt());
        assertEquals(2, p.getPositiveInt());
    }

    @Test
    public void testPutSixAndReadMultiplePages() {
        PageList p = new PageList(factory, 5);
        p.putInt(1);
        p.putInt(2);
        p.putInt(3);
        p.putInt(4);
        p.putInt(5);
        p.putInt(6);
        assertEquals(1, p.getPositiveInt());
        assertEquals(2, p.getPositiveInt());
        assertEquals(3, p.getPositiveInt());
        assertEquals(4, p.getPositiveInt());
        assertEquals(5, p.getPositiveInt());
        assertEquals(6, p.getPositiveInt());
    }

    @Test
    public void testMarkAtBeginning() {
        PageList p = new PageList(factory, 16);
        Assert.assertFalse(p.writeMarked);
        p.markForWrite();
        assertTrue(p.writeMarked);
        p.putInt(1);
        assertEquals(4, p.writePosition);
        assertEquals(4, p.currentWritePosition);
        p.moveToWriteMark();

        p.putInt(2);
        p.putInt(3);
        assertEquals(2, p.getPositiveInt());
        assertEquals(3, p.getPositiveInt());
    }

    @Test
    public void testMoveToEnd() {
        PageList p = new PageList(factory, 16);
        p.markForWrite();
        p.putInt(1);
        p.moveToWriteMark();
        p.putInt(2);
        p.moveWriteToEnd();
        p.clearWriteMark();
        p.putInt(3);
        assertEquals(2, p.getPositiveInt());
        assertEquals(3, p.getPositiveInt());
    }

    @Test
    public void testMarkInMiddle() {
        PageList p = new PageList(factory, 8);
        p.putInt(1);
        p.markForWrite();
        p.putInt(2);
        p.putInt(3);
        p.moveToWriteMark();
        p.putInt(4);
        p.clearWriteMark();
        p.moveWriteToEnd();
        p.putInt(5);
        assertEquals(1, p.getPositiveInt());
    }
}
