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

    public void put(byte[] bytes) {
        pages.mark();
        pages.put(0);
        pages.put(bytes);
        pages.moveToMark();
        lock.lock();
        try {
            pages.put(bytes.length);
        } finally {
            lock.unlock();
        }
        pages.clearMark();
        pages.moveToEnd();
    }

    public byte[] get() {
        int length;
        try {
            lock.lock();
            length = pages.get();
        } finally {
            lock.unlock();
        }
        if (length == 0) {
            return null;
        } else {
            return pages.get(length);
        }
    }

}
