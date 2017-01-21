package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.Arrays;

public final class Page {

    private static final boolean debug = false;

    private final int pageSize;
    private final MemoryMappedFile mm;

    public Page(File file, int pageSize) {
        this.pageSize = pageSize;
        this.mm = new MemoryMappedFile(file, pageSize);
    }

    public int length() {
        return pageSize;
    }

    public void put(int position, byte[] bytes, int start, int length) {
        if (debug)
            println("put at " + this.hashCode() + ":" + position + " of "
                    + Arrays.toString(Arrays.copyOfRange(bytes, start, start + length)));
        mm.putBytes(position, bytes, start, length);
    }

    public void putIntOrdered(int writePosition, int value) {
        if (debug)
            println("putIntOrdered at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putOrderedInt(writePosition, value);
    }

    public void putInt(int writePosition, int value) {
        if (debug)
            println("putInt at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putInt(writePosition, value);
    }

    public void get(byte[] dst, int offset, int readPosition, int length) {
        if (debug)
            println("getting at " + this.hashCode() + ":" + readPosition + " length=" + length);
        mm.getBytes(readPosition, dst, offset, length);
    }

    public int getIntVolatile(int readPosition) {
        int n = mm.getIntVolatile(readPosition);
        if (debug)
            println("getting int volatile at " + this.hashCode() + ":" + readPosition + "=" + n);
        return n;
    }

    public void close() {
        mm.close();
    }

    static void println(String s) {
        System.out.println(Thread.currentThread().getName() + ": " + s);
    }

    public int avail(int position) {
        return pageSize - position;
    }

    public int getInt(int readPosition) {
        return mm.getInt(readPosition);
    }

    public void putByte(int currentWritePosition, byte b) {
        mm.putByte(currentWritePosition, b);
    }

    public byte getByte(int readPosition) {
        return mm.getByte(readPosition);
    }
    
    public void force(boolean updateMetadata) {
    	mm.force(updateMetadata);
    }
}
