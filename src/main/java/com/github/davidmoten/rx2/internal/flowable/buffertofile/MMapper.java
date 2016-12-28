package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import sun.nio.ch.FileChannelImpl;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class MMapper {

    private static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;
    private static final int BYTE_ARRAY_OFFSET;

    private final String filename;
    private final long size;
    private long addr;

    static {
        try {
            unsafe = UnsafeAccess.unsafe();
            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Bundle reflection calls to get access to the given method
    private static Method getMethod(Class<?> cls, String name, Class<?>... params)
            throws Exception {
        Method m = cls.getDeclaredMethod(name, params);
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
            final RandomAccessFile backingFile = new RandomAccessFile(this.filename, "rw");
            backingFile.setLength(this.size);
            final FileChannel ch = backingFile.getChannel();
            this.addr = (Long) mmap.invoke(ch, 1, 0L, this.size);

            ch.close();
            backingFile.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MMapper(final String filename, long len) {
        this.filename = filename;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

//    // Callers should synchronize to avoid calls in the middle of this, but
//    // it is undesirable to synchronize w/ all access methods.
//    public void remap(long nLen) throws Exception {
//        unmmap.invoke(null, addr, this.size);
//        this.size = roundTo4096(nLen);
//        mapAndSetOffset();
//    }

    public void close() {
        try {
            unmmap.invoke(null, addr, this.size);
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

    public long getLong(long pos) {
        return unsafe.getLong(pos + addr);
    }

    public void putInt(long pos, int val) {
        unsafe.putInt(pos + addr, val);
    }

    public void putLong(long pos, long val) {
        unsafe.putLong(pos + addr, val);
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
    
    public void storeFence() {
        unsafe.storeFence();
    }
}