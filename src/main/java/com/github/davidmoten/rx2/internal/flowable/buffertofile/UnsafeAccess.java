package com.github.davidmoten.rx2.internal.flowable.buffertofile;

import java.lang.reflect.Field;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public final class UnsafeAccess {

    private UnsafeAccess() {
        // prevent instantiation
    }

    private static Unsafe unsafe;

    static {
        try {
            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            unsafe = (Unsafe) singleoneInstanceField.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Unsafe unsafe() {
        return unsafe;
    }

}
