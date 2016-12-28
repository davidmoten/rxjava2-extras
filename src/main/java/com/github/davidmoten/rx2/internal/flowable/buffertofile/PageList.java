package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;

public class PageList {

    private static final byte[] EMPTY = new byte[0];

    private final Callable<File> fileFactory;
    private final int pageSize;

    // read queue
    private final SimplePlainQueue<Page> queue = new SpscArrayQueue<Page>(16);

    // keep a record of page sequence required for when we move to bookmark and
    // write from there (possibly across many pages)
    // TODO use non-concurrent queue
    private final SimplePlainQueue<Page> replayWriteQueue = new SpscArrayQueue<Page>(16);

    // keep a record of page sequence required for when we move to bookmark and
    // write from there (possibly across many pages)
    boolean writeMarked;
    boolean writingFromMark;
    int markWritePosition; // replayQueue has start page

    Page currentWritePage;
    int currentWritePosition;

    Page writePage;
    int writePosition;

    Page readPage;
    int readPosition;

    public PageList(Callable<File> fileFactory, int pageSize) {
        Preconditions.checkArgument(pageSize >= 4);
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
    }

    public void markForWrite() {
        writeMarked = true;
        replayWriteQueue.clear();
        replayWriteQueue.offer(currentWritePage());
        markWritePosition = writePosition;
    }

    public void moveToWriteMark() {
        writingFromMark = true;
        currentWritePage = replayWriteQueue.poll();
        currentWritePosition = markWritePosition;
    }

    public void moveWriteToEnd() {
        writingFromMark = false;
        currentWritePage = writePage;
        currentWritePosition = writePosition;
    }

    public void clearWriteMark() {
        writeMarked = false;
        replayWriteQueue.clear();
    }

    public void putInt(int value) {
        put(intToByteArray(value));
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
            if (writeMarked && before != page) {
                replayWriteQueue.offer(page);
            }
            if (!this.writingFromMark) {
                writePosition = currentWritePosition;
            }
        }
    }

    private Page currentWritePage() {
        if (writingFromMark && currentWritePosition == pageSize) {
            currentWritePage = replayWriteQueue.poll();
            currentWritePosition = 0;
        }
        if (currentWritePage == null || currentWritePosition == pageSize) {
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

    public int getPositiveInt() {
        byte[] bytes = get(4);
        if (bytes.length == 0) {
            return -1;
        } else {
            return byteArrayToInt(bytes);
        }
    }

    public byte[] get(int length) {
        int len = length;
        byte[] result = new byte[length];
        while (len > 0) {
            if (readPage() == null) {
                return EMPTY;
            }
            int avail = readPage.length() - readPosition;
            int count = Math.min(avail, len);
            readPage.get(result, length - len, readPosition, count);
            readPosition += count;
            len -= count;
        }
        return result;
    }

    private Page readPage() {
        if (readPage == null || readPosition >= pageSize) {
            if (readPage != null) {
                readPage.close();
            }
            readPage = queue.poll();
            readPosition = readPosition % pageSize;
        }
        return readPage;
    }

    private static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8),
                (byte) value };
    }

    private static int byteArrayToInt(byte[] bytes) {
        return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8
                | (bytes[3] & 0xFF);
    }

    public void putOrdered(int length) {
        putInt(length);
    }

    public void putByte(byte b) {
        // TODO reuse mutable single item array
        put(new byte[] { b });
    }

    public byte getByte() {
        return get(1)[0];
    }

    public void moveReadPosition(int forward) {
        readPosition+=forward;
    }

}
