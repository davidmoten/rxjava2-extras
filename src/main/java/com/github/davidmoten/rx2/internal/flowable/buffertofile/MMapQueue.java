package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;

public class MMapQueue {

    private final Callable<File> fileFactory;
    private final int pageSize;
    private final PageList pages;

    public MMapQueue(Callable<File> fileFactory, int pageSize) {
        this.fileFactory = fileFactory;
        this.pageSize = pageSize;
        this.pages = new PageList(fileFactory, pageSize);
    }
    
}
