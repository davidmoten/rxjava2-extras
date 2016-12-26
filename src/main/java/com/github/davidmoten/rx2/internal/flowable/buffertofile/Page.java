package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Page {

    private final int pageSize;
    private int writePosition;
    private final MappedByteBuffer bb;
    // initiated on demand, is for single consumer so should be initiated only
    // once despite visibility
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

}
