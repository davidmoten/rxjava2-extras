package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

@SuppressWarnings("restriction")
public final class MemoryMappedFile {

    private static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;
    private static final int BYTE_ARRAY_OFFSET;

    private final File file;
    private final long size;
    private long addr;

    static {
        unsafe = UnsafeAccess.unsafe();
        mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
        unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
        BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    }

    public MemoryMappedFile(File file, long len) {
        this.file = file;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

    // visible for testing
    // Bundle reflection calls to get access to the given method
    static Method getMethod(Class<?> cls, String name, Class<?>... params) {
        Method m;
        try {
            m = cls.getDeclaredMethod(name, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        m.setAccessible(true);
        return m;
    }

    // Round to next 4096 bytes
    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    // Given that the location and size have been set, map that location
    // for the given length and set this.addr to the returned offset
    private void mapAndSetOffset() {
        try {
            final RandomAccessFile backingFile = new RandomAccessFile(this.file, "rw");
            backingFile.setLength(this.size);
            final FileChannel ch = backingFile.getChannel();
            this.addr = (Long) mmap.invoke(ch, 1, 0L, this.size);

            ch.close();
            backingFile.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // // Callers should synchronize to avoid calls in the middle of this, but
    // // it is undesirable to synchronize w/ all access methods.
    // public void remap(long nLen) throws Exception {
    // unmmap.invoke(null, addr, this.size);
    // this.size = roundTo4096(nLen);
    // mapAndSetOffset();
    // }

    public void close() {
        try {
            unmmap.invoke(null, addr, this.size);
            if (!file.delete()) {
                throw new RuntimeException("could not delete " + file);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public int getInt(long pos) {
        return unsafe.getInt(pos + addr);
    }

    public void putByte(long pos, byte val) {
        unsafe.putByte(pos + addr, val);
    }
    
    public void putInt(long pos, int val) {
        unsafe.putInt(pos + addr, val);
    }

    public void putOrderedInt(long pos, int val) {
        unsafe.putOrderedInt(null, pos + addr, val);
    }

    public int getIntVolatile(long pos) {
        return unsafe.getIntVolatile(null, pos + addr);
    }

    // May want to have offset & length within data as well, for both of these
    public void getBytes(long pos, byte[] data, long offset, long length) {
        unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET + offset, length);
    }

    public void putBytes(long pos, byte[] data, long offset, long length) {
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + addr, length);
    }

}