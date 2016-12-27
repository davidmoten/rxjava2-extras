package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class Page {

    private final int pageSize;
    private MMapper mm;

    public Page(File file, int pageSize) {
        this.pageSize = pageSize;
        this.mm = new MMapper(file.getAbsolutePath(), pageSize);
    }

    public int length() {
        return pageSize;
    }

    public void put(int position, byte[] bytes, int start, int length) {
        mm.putBytes(position, bytes, start, length);
    }

    public void get(byte[] dst, int offset, int readPosition, int length) {
        mm.getBytes(readPosition, dst, offset, length);
    }

    public void close() {
        mm.close();
    }
    
    private static void unmap(MappedByteBuffer buffer) {
        Cleaner.clean(buffer);
    }

    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer")
                        .getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
            if (buffer == null)
                return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }

}
