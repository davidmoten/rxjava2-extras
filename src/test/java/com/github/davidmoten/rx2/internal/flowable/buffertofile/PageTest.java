package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

public class PageTest {

    @Test
    public void testPutAndRead() {
        Page page = new Page(new File("target/p1"), 100);
        byte[] bytes = new byte[] { 0, 1, 2, 3 };
        page.put(0, bytes, 0, bytes.length);
        byte[] a = new byte[4];
        page.read(a, 0, 0, 4);
        Assert.assertArrayEquals(bytes, a);
        a = new byte[2];
        page.read(a, 0, 2, 2);
        Assert.assertArrayEquals(new byte[] { 2, 3 }, a);
    }

}
