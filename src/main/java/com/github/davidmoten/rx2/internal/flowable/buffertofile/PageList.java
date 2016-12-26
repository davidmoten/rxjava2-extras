package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;

public class PageList {

    private final Callable<File> fileFactory;
    private final int pageSize;

    // read queue
    private final SimplePlainQueue<Page> queue = new SpscArrayQueue<Page>(16);

    // keep a record of page sequence required for when we move to bookmark and
    // write from there (possibly across many pages)
    private final SimplePlainQueue<Page> replayQueue = new SpscArrayQueue<Page>(16);

    boolean marked;
    boolean writingFromMark;
    int markWritePosition; // replayQueue has start page

    int writePosition;
    Page writePage;

    Page currentWritePage;
    int currentWritePosition;

    Page readPage;
    int readPosition;

    public PageList(Callable<File> fileFactory, int pageSize) {
        Preconditions.checkArgument(pageSize >= 4);
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
    }

    public void mark() {
        marked = true;
        replayQueue.clear();
        replayQueue.offer(currentWritePage());
        markWritePosition = writePosition;
    }

    public void put(int value) {
        put(intToByteArray(value));
    }

    public void clearMark() {
        marked = false;
        replayQueue.clear();
    }
    
    public void moveWritePositionToEnd() {
        writingFromMark = false;
        currentWritePage = writePage;
        currentWritePosition = writePosition;
    }

    public void put(byte[] bytes) {
        int length = bytes.length;
        int start = 0;
        while (length > 0) {
            Page before = currentWritePage;
            Page page = currentWritePage();
            int avail = page.length() - currentWritePosition;
            int len = Math.min(avail, length);
            page.put(currentWritePosition, bytes, start, len);
            currentWritePosition += len;
            start += len;
            length -= len;
            if (marked && before != page) {
                replayQueue.offer(page);
            } 
            if (!this.writingFromMark) {
                writePosition = currentWritePosition;
            }
        }
    }

    public void moveToMark() {
        writingFromMark = true;
        currentWritePage = replayQueue.poll();
        currentWritePosition = markWritePosition;
    }

    private Page currentWritePage() {
        if (writingFromMark && currentWritePosition == pageSize) {
            currentWritePage = replayQueue.poll();
            currentWritePosition = 0;
        } else if (currentWritePage == null || currentWritePosition == pageSize) {
            File file;
            try {
                file = fileFactory.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            currentWritePage = new Page(file, pageSize);
            currentWritePosition = 0;
            writePage = currentWritePage;
            queue.offer(currentWritePage);
        }
        return currentWritePage;
    }

    public int get() {
        byte[] bytes = get(4);
        return byteArrayToInt(bytes);
    }

    public byte[] get(int length) {
        int len = length;
        byte[] result = new byte[length];
        while (len > 0) {
            if (readPage == null || readPosition == pageSize) {
                readPage = queue.poll();
                readPosition = 0;
            }
            int avail = readPage.length() - readPosition;
            int count = Math.min(avail, len);
            readPage.get(result, length - len, readPosition, count);
            readPosition += count;
            len -= count;
        }
        return result;
    }

    private static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                (byte) value };
    }

    private static int byteArrayToInt(byte[] bytes) {
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8
                | (bytes[3] & 0xFF);
    }
}
