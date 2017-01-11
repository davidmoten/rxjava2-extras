package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;

public final class Pages {

    private static final boolean CHECK = false;

    private static final int QUEUE_INITIAL_CAPACITY = 16;
    private static final byte[] EMPTY = new byte[0];

    private final Callable<File> fileFactory;
    private final int pageSize;

    // read queue must be SPSC because is added to from the write thread
    private final SimplePlainQueue<Page> queue = new SpscLinkedArrayQueue<Page>(
            QUEUE_INITIAL_CAPACITY);

    Page writePage;
    int writePosition;

    Page readPage;
    int readPosition;

    Page markPage;
    int markPosition;

    public Pages(Callable<File> fileFactory, int pageSize) {
        Preconditions.checkArgument(pageSize >= 4);
        Preconditions.checkArgument(pageSize % 4 == 0);
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
    }

    public int avail() {
        return writePage().avail(writePosition);
    }

    public void markForRewriteAndAdvance4Bytes() {
        markPage = writePage();
        markPosition = writePosition;
        writePosition += 4;
//         putInt(markPage, 0);
    }

    public void putInt(int value) {
        putInt(writePage(), value);
    }

    private void putInt(Page page, int value) {
        if (CHECK) {
            int avail = page.length() - writePosition;
            if (avail < 0)
                throw new RuntimeException("unexpected");
        }
        page.putInt(writePosition, value);
        writePosition += 4;
    }

    public void put(byte[] bytes, int offset, int length) {
        Page page = writePage();
        if (CHECK) {
            if (length == 0)
                throw new IllegalArgumentException();
            int avail = page.length() - writePosition;
            if (avail < 0)
                throw new RuntimeException("unexpected");
        }
        page.put(writePosition, bytes, offset, length);
        writePosition += length;
    }

    public void putIntOrderedAtRewriteMark(int value) {
        // if there is any space at all in current page then it will be enough
        // for 4 bytes because we pad all offerings to the queue
        markPage.putIntOrdered(markPosition, value);
        markPage = null;
    }

    private Page writePage() {
        if (writePage == null || writePosition == pageSize) {
            createNewPage();
        }
        return writePage;
    }

    private void createNewPage() {
        File file;
        try {
            file = fileFactory.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        writePage = new Page(file, pageSize);
        writePosition = 0;
        queue.offer(writePage);
        // System.out.println(Thread.currentThread().getName() + ": created
        // page "
        // + currentWritePage.hashCode());
    }

    public int getInt() {
        if (readPage() == null) {
            return -1;
        }
        int rp = readPosition;
        if (CHECK) {
            int avail = readPage.length() - rp;
            if (avail < 4)
                throw new RuntimeException("unexpected");
        }
        readPosition = rp + 4;
        return readPage.getInt(rp);
    }

    public byte[] get(int length) {
        byte[] result = new byte[length];
        if (readPage() == null) {
            return EMPTY;
        }
        if (CHECK) {
            int avail = readPage.length() - readPosition;
            if (avail < length)
                throw new RuntimeException("unexpected");
        }
        readPage.get(result, 0, readPosition, length);
        readPosition += length;
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

    public void putByte(byte b) {
        Page page = writePage();
        if (CHECK) {
            int avail = page.length() - writePosition;
            if (avail < 0)
                throw new RuntimeException("unexpected");
        }
        page.putByte(writePosition, b);
        writePosition += 1;
    }

    public byte getByte() {
        Page page = readPage();
        if (CHECK) {
            int avail = page.length() - readPosition;
            if (avail < 1)
                throw new RuntimeException("unexpected");
        }
        byte result = page.getByte(readPosition);
        readPosition += 1;
        return result;
    }

    public void moveReadPosition(int forward) {
        readPosition += forward;
    }

    public int getIntVolatile() {
        if (readPage() == null) {
            return -1;
        } else {
            int result = readPage.getIntVolatile(readPosition);
            readPosition += 4;
            return result;
        }
    }

    public void moveWritePosition(int forward) {
        writePosition += forward;
    }

    public void close() {
        if (readPage != null) {
            readPage.close();
            readPage = null;
        }
        Page page;
        while ((page = queue.poll()) != null) {
            page.close();
        }
    }

}
