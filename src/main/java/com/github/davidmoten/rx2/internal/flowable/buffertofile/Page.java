package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

public class Page {

    private static final boolean debug = false;

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
        if (debug)
            System.out.println("put at " + this.hashCode() + ":" + position + " of "
                    + Arrays.toString(Arrays.copyOfRange(bytes, start, start + length)));
        mm.putBytes(position, bytes, start, length);
    }

    public void putIntOrdered(int writePosition, int value) {
        if (debug)
        System.out.println(
                "putIntOrdered at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putOrderedInt(writePosition, value);
    }

    public void putInt(int writePosition, int value) {
        if (debug)
        System.out.println("putInt at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putInt(writePosition, value);
    }

    public void get(byte[] dst, int offset, int readPosition, int length) {
        if (debug)
        System.out.println(
                "getting at " + this.hashCode() + ":" + readPosition + " length=" + length);
        mm.getBytes(readPosition, dst, offset, length);
    }

    public int getIntVolatile(int readPosition) {
        int n = mm.getIntVolatile(readPosition);
        if (debug)
        System.out.println(
                "getting int volatile at " + this.hashCode() + ":" + readPosition + "=" + n);
        return n;
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
