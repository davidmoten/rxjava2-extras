package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Page {

    private final int pageSize;
    // initiated on demand, is for single consumer so should be initiated only
    // once despite visibility effects
    private MappedByteBuffer readBb;

    public Page(File file, int pageSize) {
        this.pageSize = pageSize;
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
            bb = channel.map(READ_WRITE, 0, this.pageSize);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (channel != null)
                try {
                    channel.close();
                } catch (IOException e) {
                    // ignore
                }
            if (raf != null)
                try {
                    raf.close();
                } catch (IOException e) {
                    // ignore
                }
        }
    }

    public int length() {
        return pageSize;
    }

    public void put(int position, byte[] bytes, int start, int length) {
        bb.position(position);
        bb.put(bytes, start, length);
    }

    public void get(byte[] dst, int offset, int readPosition, int length) {
        if (readBb == null) {
            readBb = (MappedByteBuffer) bb.duplicate();
        }
        readBb.position(readPosition);
        readBb.get(dst, offset, length);
    }

    public void close() {
        unmap(bb);
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
