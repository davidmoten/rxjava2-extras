package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class PageForced implements PageI {

    private ThreadLocalByteBuffer reader;
    private final MappedByteBuffer writer;
    private final int pageSize;
    private final File file;
    private final RandomAccessFile raf;

    public PageForced(File file, int pageSize) {
        this.file = file;
        this.pageSize = pageSize;
        try {
            raf = new RandomAccessFile(file, "rw");
            writer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, pageSize);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        reader = new ThreadLocalByteBuffer(writer);
    }

    @Override
    public int length() {
        return pageSize;
    }

    @Override
    public File file() {
        return file;
    }

    @Override
    public void put(int position, byte[] bytes, int start, int length) {
        writer.position(position);
        writer.put(bytes, start, length);
    }

    @Override
    public void putIntOrdered(int position, int value) {
        writer.position(position);
        writer.putInt(value);
        writer.force();
    }

    @Override
    public void putInt(int position, int value) {
        writer.position(position);
        writer.putInt(value);
    }

    @Override
    public void get(byte[] dst, int offset, int position, int length) {
        reader.get().position(position);
        reader.get().get(dst, offset, length);
    }

    @Override
    public int getIntVolatile(int position) {
        return getInt(position);
    }

    @Override
    public int avail(int position) {
        return pageSize - position;
    }

    @Override
    public int getInt(int position) {
        reader.get().position(position);
        return reader.get().getInt();
    }

    @Override
    public void putByte(int position, byte b) {
        writer.position(position);
        writer.put(b);
    }

    @Override
    public byte getByte(int position) {
        reader.get().position(position);
        return reader.get().get();
    }

    @Override
    public void forceWrite() {
        writer.force();
    }

    @Override
    public void close() {
        try {
            raf.close();
            // set to null so all thread locals can be gc'd
            reader = null;
            if (!file.delete()) {
                throw new RuntimeException("could not delete " + file);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        
        private final ByteBuffer source;

        public ThreadLocalByteBuffer(ByteBuffer src) {
            source = src;
        }

        @Override
        protected synchronized ByteBuffer initialValue() {
            return source.duplicate();
        }
    }

}
