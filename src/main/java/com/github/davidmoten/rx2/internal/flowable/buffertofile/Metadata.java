package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public final class Metadata {

	private static final int LENGTH = 32;

	private final MappedByteBuffer b;
	private final RandomAccessFile raf;
	private final FileChannel channel;

	private int startPageNumber;
	private int writePageNumber;
	private long writePosition;

	public Metadata(File file) {
		try {
			raf = new RandomAccessFile(file, "rw");
			channel = raf.getChannel();
			b = channel.map(FileChannel.MapMode.READ_WRITE, 0, LENGTH);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Metadata startPageNumber(int startPageNumber) {
		this.startPageNumber = startPageNumber;
		b.position(0);
		b.putInt(startPageNumber);
		return this;
	}

	public Metadata writePageNumber(int writePageNumber) {
		this.writePageNumber = writePageNumber;
		b.position(4);
		b.putInt(writePageNumber);
		return this;
	}

	public Metadata writePosition(int writePosition) {
		this.writePosition = writePosition;
		b.position(8);
		b.putInt(writePosition);
		return this;
	}

	public int startPageNumber() {
		return startPageNumber;
	}

	public int writePageNumber() {
		return writePageNumber;
	}

	public long writePosition() {
		return writePosition;
	}
	
	public void close() {
		try {
			channel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
