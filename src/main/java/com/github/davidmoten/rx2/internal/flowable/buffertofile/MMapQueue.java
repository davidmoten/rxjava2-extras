package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

public class MMapQueue {

    private final PageList pages;
    private final ReentrantLock lock = new ReentrantLock();

    public MMapQueue(Callable<File> fileFactory, int pageSize) {
        this.pages = new PageList(fileFactory, pageSize);
    }

    private static final int SIZE_MESSAGE_SIZE_FIELD = 4;
    private static final int SIZE_PADDING_SIZE_FIELD = 1;
    private static final int SIZE_MESSAGE_TYPE_FIELD = 1;
    private static final int HEADER_BYTES = SIZE_MESSAGE_SIZE_FIELD + SIZE_MESSAGE_TYPE_FIELD
            + SIZE_PADDING_SIZE_FIELD + SIZE_MESSAGE_SIZE_FIELD;
    private static final int ALIGN_BYTES = 4;

    @SuppressWarnings("restriction")
    public void offer(byte[] bytes) {
//        System.out.println("writing " + bytes.length + " bytes");
        int rem = (bytes.length + SIZE_PADDING_SIZE_FIELD) % ALIGN_BYTES;
        final int padding;
        if (rem == 0) {
            padding = 0;
        } else {
            padding = ALIGN_BYTES - rem;
        }
        pages.markForWrite();
        pages.putInt(0);// messageSize
        UnsafeAccess.unsafe().storeFence();
        pages.putByte((byte) padding);
        if (padding > 0) {
            pages.put(new byte[padding]);
        }
        pages.put(bytes);
        pages.moveToWriteMark();
        try {
//            lock.lock();
            // TODO ordered put
            pages.putIntOrdered(bytes.length);
        } finally {
//            lock.unlock();
        }
        pages.clearWriteMark();
        pages.moveWriteToEnd();
    }

    public byte[] poll() {
//        System.out.println("reading");
        int length;
        try {
//            lock.lock();
            // TODO volatile read
            length = pages.getPositiveIntVolatile();
        } finally {
//            lock.unlock();
        }
        if (length == 0) {
            pages.moveReadPosition(-4);
            return null;
        } else if (length == -1) {
            return null;
        } else {
            byte padding = pages.getByte();
            if (padding > 0) {
                pages.moveReadPosition(padding);
            }
            return pages.get(length);
        }
    }

}
