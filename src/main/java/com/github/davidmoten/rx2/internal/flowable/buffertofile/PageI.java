package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;

public interface PageI {

    int length();

    File file();

    void put(int position, byte[] bytes, int start, int length);

    void putIntOrdered(int writePosition, int value);

    void putInt(int writePosition, int value);

    void get(byte[] dst, int offset, int readPosition, int length);

    int getIntVolatile(int readPosition);

    void close();

    int avail(int position);

    int getInt(int readPosition);

    void putByte(int currentWritePosition, byte b);

    byte getByte(int readPosition);

    void forceWrite();

}