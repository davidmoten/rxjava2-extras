package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;

import com.google.common.base.Preconditions;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;

public class PageList {

    private final Callable<File> fileFactory;
    private final int pageSize;
    private final SimplePlainQueue<Page> queue = new SpscArrayQueue<Page>(16);
    // keep a record of page sequence required for when we move to bookmark and
    // write from there (possibly across many pages)
    private final SimplePlainQueue<Page> replayQueue = new SpscArrayQueue<Page>(16);

    private boolean writeFromMark;
    private int markWritePosition; // replayQueue has start page

    private int writePosition;
    private Page writePage;

    private Page currentWritePage;
    private int currentWritePosition;

    private Page readPage;
    private int readPosition;

    public PageList(Callable<File> fileFactory, int pageSize) {
        Preconditions.checkArgument(pageSize >= 8);
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
    }

    public void markWritePosition() {
        replayQueue.offer(currentWritePage);
        markWritePosition = writePosition;
    }

    public void put(int value) {
        put(intToByteArray(value));
    }

    public void moveWritePositionToEnd() {
        writeFromMark = false;
        currentWritePage = writePage;
        currentWritePosition = writePosition;
    }

    public void put(byte[] bytes) {
        int length = bytes.length;
        int start = 0;
        while (length > 0) {
            Page page = currentWritePage();
            int avail = page.availableWriteBytes();
            int len = Math.max(avail, length);
            page.put(currentWritePosition, bytes, start, len);
            length -= len;
            if (!writeFromMark) {
                replayQueue.offer(page);
            }
        }
    }

    public void moveWritePositionToMark() {
        writeFromMark = true;
        currentWritePage = replayQueue.poll();
        currentWritePosition = markWritePosition;
    }

    private Page currentWritePage() {
        if (writeFromMark && currentWritePosition == pageSize) {
            currentWritePage = replayQueue.poll();
            currentWritePosition = 0;
        } else if (currentWritePage == null) {
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

    public byte[] read(int length) {
        int len = 0;
        byte[] result = new byte[length];
        while (len < length) {
            if (readPage == null || readPosition == pageSize) {
                readPage = queue.poll();
                readPosition = 0;
            }
            int avail = readPage.length() - readPosition;
            int count = Math.min(avail, len);
            readPage.read(result, len, readPosition, count);
            readPosition += count;
            len += count;
        }
        return result;
    }

    private static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8),
                (byte) value };
    }
}
