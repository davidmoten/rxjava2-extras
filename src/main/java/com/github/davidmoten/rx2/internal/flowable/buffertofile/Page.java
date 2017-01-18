package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.Arrays;

public final class Page implements PageI {

    private static final boolean debug = false;

    private final int pageSize;
    private final MemoryMappedFile mm;
    private final File file;

    public Page(File file, int pageSize) {
        this.pageSize = pageSize;
        this.file = file;
        this.mm = new MemoryMappedFile(file, pageSize);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#length()
     */
    @Override
    public int length() {
        return pageSize;
    }
    
    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#file()
     */
    @Override
    public File file() {
        return file;
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#put(int, byte[], int, int)
     */
    @Override
    public void put(int position, byte[] bytes, int start, int length) {
        if (debug)
            println("put at " + this.hashCode() + ":" + position + " of "
                    + Arrays.toString(Arrays.copyOfRange(bytes, start, start + length)));
        mm.putBytes(position, bytes, start, length);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#putIntOrdered(int, int)
     */
    @Override
    public void putIntOrdered(int writePosition, int value) {
        if (debug)
            println("putIntOrdered at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putOrderedInt(writePosition, value);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#putInt(int, int)
     */
    @Override
    public void putInt(int writePosition, int value) {
        if (debug)
            println("putInt at " + this.hashCode() + ":" + writePosition + " of " + value);
        mm.putInt(writePosition, value);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#get(byte[], int, int, int)
     */
    @Override
    public void get(byte[] dst, int offset, int readPosition, int length) {
        if (debug)
            println("getting at " + this.hashCode() + ":" + readPosition + " length=" + length);
        mm.getBytes(readPosition, dst, offset, length);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#getIntVolatile(int)
     */
    @Override
    public int getIntVolatile(int readPosition) {
        int n = mm.getIntVolatile(readPosition);
        if (debug)
            println("getting int volatile at " + this.hashCode() + ":" + readPosition + "=" + n);
        return n;
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#close()
     */
    @Override
    public void close() {
        mm.close();
    }

    static void println(String s) {
        System.out.println(Thread.currentThread().getName() + ": " + s);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#avail(int)
     */
    @Override
    public int avail(int position) {
        return pageSize - position;
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#getInt(int)
     */
    @Override
    public int getInt(int readPosition) {
        return mm.getInt(readPosition);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#putByte(int, byte)
     */
    @Override
    public void putByte(int currentWritePosition, byte b) {
        mm.putByte(currentWritePosition, b);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#getByte(int)
     */
    @Override
    public byte getByte(int readPosition) {
        return mm.getByte(readPosition);
    }

    /* (non-Javadoc)
     * @see com.github.davidmoten.rx2.internal.flowable.buffertofile.PageI#force()
     */
    @Override
    public void forceWrite() {
        //do nothing
    }
}
