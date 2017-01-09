package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import org.junit.Test;

public class MemoryMappedFileTest {

    @Test(expected = RuntimeException.class)
    public void testGetMethod() {
        MemoryMappedFile.getMethod(MemoryMappedFileTest.class, "hello");
    }

}
