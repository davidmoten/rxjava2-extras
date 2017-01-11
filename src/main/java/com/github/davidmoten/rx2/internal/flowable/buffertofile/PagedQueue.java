package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.davidmoten.guavamini.Preconditions;

@SuppressWarnings("serial")
public final class PagedQueue extends AtomicInteger {

    public static final boolean debug = true;

    private static final int EXTRA_PADDING_LIMIT = 64;
    private static final int SIZE_MESSAGE_SIZE_FIELD = 4;
    private static final int SIZE_PADDING_SIZE_FIELD = 1;
    private static final int SIZE_MESSAGE_TYPE_FIELD = 1;
    private static final int ALIGN_BYTES = 4;
    public static final int MAX_PADDING_PER_MESSAGE = 32;
    private static final int SIZE_HEADER_PRIMARY_PART = SIZE_MESSAGE_SIZE_FIELD
            + SIZE_MESSAGE_TYPE_FIELD + SIZE_PADDING_SIZE_FIELD;

    private final Pages pages;

    private boolean readingFragments;
    private byte[] messageBytesAccumulated;
    private int indexBytesAccumulated;

    public PagedQueue(Callable<File> fileFactory, int pageSize) {
        this.pages = new Pages(fileFactory, pageSize);
    }

    public void offer(byte[] bytes) {
        if (getAndIncrement() != 0) {
            return;
        }
        try {
            int padding = padding(bytes.length);
            int avail = pages.avail();
            int fullMessageSize = fullMessageSize(bytes.length, padding);
            // header plus 4 bytes
            int availAfter = avail - fullMessageSize;

            if (availAfter >= 0) {
                if (availAfter <= MAX_PADDING_PER_MESSAGE) {
                    padding += availAfter;
                }
                writeFullMessage(bytes, padding);
            } else {
                writeFragments(bytes, avail);
            }
        } finally {
            decrementAndGet();
        }
    }

    private void writeFullMessage(byte[] bytes, int padding) {
        write(bytes, 0, bytes.length, padding, MessageType.FULL_MESSAGE, bytes.length);
    }

    private void writeFragments(byte[] bytes, int avail) {
        int start = 0;
        int length = bytes.length;
        do {
            int extraHeaderBytes = start == 0 ? 4 : 0;
            int count = Math.min(avail - 8 - extraHeaderBytes, length);
            int padding = padding(count);
            int remaining = Math.max(0, avail - count - 6 - padding - extraHeaderBytes);
            if (remaining <= EXTRA_PADDING_LIMIT)
                padding += remaining;
            System.out.println(String.format(
                    "length=%s,start=%s,count=%s,padding=%s,remaining=%s,extraHeaderBytes=%s",
                    length, start, count, padding, remaining, extraHeaderBytes));
            write(bytes, start, count, padding, MessageType.FRAGMENT, bytes.length);
            start += count;
            length -= count;
            if (length > 0) {
                avail = pages.avail();
            }
        } while (length > 0);
    }

    private int fullMessageSize(int payloadLength, int padding) {
        return SIZE_HEADER_PRIMARY_PART + padding + payloadLength;
    }

    private static int padding(int payloadLength) {
        int rem = (payloadLength + SIZE_PADDING_SIZE_FIELD + SIZE_MESSAGE_TYPE_FIELD) % ALIGN_BYTES;
        int padding;
        if (rem == 0) {
            padding = 0;
        } else {
            padding = ALIGN_BYTES - rem;
        }
        return padding;
    }

    @SuppressWarnings("restriction")
    private void write(byte[] bytes, int offset, int length, int padding,
            final MessageType messageType, int totalLength) {
        Preconditions.checkArgument(length != 0);
        pages.markForRewriteAndAdvance4Bytes();// messageSize left as 0
        // storeFence not required at this point like Aeron uses.
        UnsafeAccess.unsafe().storeFence();
        if (padding == 2) {
            pages.putInt((messageType.value() << 0) | (((byte) padding) & 0xFF) << 8);
        } else {
            pages.putByte(messageType.value()); // message type
            pages.putByte((byte) padding);
            if (padding > 0) {
                pages.moveWritePosition(padding);
            }
        }
        if (messageType == MessageType.FRAGMENT && offset == 0) {
            // first fragment only of a sequence of fragments
            pages.putInt(totalLength);
        }
        pages.put(bytes, offset, length);
        // now that the message bytes are written we can set the length field in
        // the header to indicate that the message is ready to be read
        pages.putIntOrderedAtRewriteMark(length);
    }

    public byte[] poll() {
        // loop here accumulating fragments if necessary
        while (true) {
            int length = pages.getIntVolatile();
            if (length == 0) {
                // not ready for read
                pages.moveReadPosition(-4);
                return null;
            } else if (length == -1) {
                // at end of read queue
                // System.out.println("at end of read queue");
                return null;
            } else {
                MessageType messageType;
                byte padding;
                if (length % 4 == 0) {
                    // read message type and padding in one int read
                    int i = pages.getInt();
                    messageType = MessageType.from((byte) i);
                    padding = (byte) (i >> 8);
                    if (padding > 2) {
                        pages.moveReadPosition(padding - 2);
                    }
                } else {
                    // read message type and padding separately
                    messageType = MessageType.from(pages.getByte());
                    padding = pages.getByte();
                    if (padding > 0) {
                        pages.moveReadPosition(padding);
                    }
                }
                if (!readingFragments && messageType == MessageType.FRAGMENT) {
                    // is first fragment
                    int lengthRemaining = pages.getInt();
                    if (messageBytesAccumulated == null) {
                        messageBytesAccumulated = new byte[lengthRemaining];
                        indexBytesAccumulated = 0;
                    }
                    readingFragments = true;
                }
                byte[] result = pages.get(length);
                if (result.length == 0) {
                    return null;
                } else {
                    if (readingFragments) {
                        System.arraycopy(result, 0, messageBytesAccumulated, indexBytesAccumulated,
                                result.length);
                        indexBytesAccumulated += result.length;
                        if (indexBytesAccumulated == messageBytesAccumulated.length) {
                            readingFragments = false;
                            byte[] b = messageBytesAccumulated;
                            messageBytesAccumulated = null;
                            return b;
                        }
                    } else {
                        return result;
                    }
                }
            }
        }
    }

    private void closeWrite() {
        incrementAndGet();
        while (!compareAndSet(1, 2))
            ;
    }

    public void close() {
        // to get to here no more reads will happen because close is called from
        // the drain loop
        closeWrite();
        pages.close();
        messageBytesAccumulated = null;
    }

    private static enum MessageType {

        FULL_MESSAGE(0), FRAGMENT(1);

        private final byte value;

        private MessageType(int value) {
            this.value = (byte) value;
        }

        byte value() {
            return value;
        }

        static MessageType from(byte b) {
            if (b == 0)
                return MessageType.FULL_MESSAGE;
            else if (b == 1)
                return MessageType.FRAGMENT;
            else
                throw new RuntimeException("unexpected");
        }
    }

}
