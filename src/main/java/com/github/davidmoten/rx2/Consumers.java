package com.github.davidmoten.rx2;

import java.util.List;

import io.reactivex.functions.LongConsumer;

public final class Consumers {

    private Consumers() {
        // prevent instantiation
    }
    
    public static LongConsumer addLongTo(final List<Long> list) {
        return new LongConsumer() {

            @Override
            public void accept(long t) throws Exception {
                list.add(t);
            }
            
        };
    }

}
