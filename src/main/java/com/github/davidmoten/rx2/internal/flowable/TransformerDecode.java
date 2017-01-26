package com.github.davidmoten.rx2.internal.flowable;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public final class TransformerDecode {

    private TransformerDecode() {
        // prevent instantiation
    }

    public static FlowableTransformer<byte[], String> decode(final CharsetDecoder decoder,
            BackpressureStrategy backpressureStrategy, int batchSize) {
        Callable<ByteBuffer> initialState = new Callable<ByteBuffer>() {

            @Override
            public ByteBuffer call() {
                return null;
            }
        };
        Function3<ByteBuffer, byte[], FlowableEmitter<String>, ByteBuffer> transition = new Function3<ByteBuffer, byte[], FlowableEmitter<String>, ByteBuffer>() {

            @Override
            public ByteBuffer apply(ByteBuffer last, byte[] next, FlowableEmitter<String> o) {
                Result result = process(next, last, false, decoder, o);
                return result.leftOver;
            }
        };
        BiPredicate<ByteBuffer, FlowableEmitter<String>> completion = new BiPredicate<ByteBuffer, FlowableEmitter<String>>() {

            @Override
            public boolean test(ByteBuffer last, FlowableEmitter<String> subscriber) {
                return process(null, last, true, decoder, subscriber).canEmitFurther;
            }
        };

        return com.github.davidmoten.rx2.FlowableTransformers.stateMachine(initialState, transition, completion,
                backpressureStrategy, batchSize);
    }

    private static final class Result {
        final ByteBuffer leftOver;
        final boolean canEmitFurther;

        Result(ByteBuffer leftOver, boolean canEmitFurther) {
            this.leftOver = leftOver;
            this.canEmitFurther = canEmitFurther;
        }

    }

    public static Result process(byte[] next, ByteBuffer last, boolean endOfInput, CharsetDecoder decoder,
            FlowableEmitter<String> emitter) {
        if (emitter.isCancelled())
            return new Result(null, false);

        ByteBuffer bb;
        if (last != null) {
            if (next != null) {
                // merge leftover in front of the next bytes
                bb = ByteBuffer.allocate(last.remaining() + next.length);
                bb.put(last);
                bb.put(next);
                bb.flip();
            } else { // next == null
                bb = last;
            }
        } else { // last == null
            if (next != null) {
                bb = ByteBuffer.wrap(next);
            } else { // next == null
                return new Result(null, true);
            }
        }

        CharBuffer cb = CharBuffer.allocate((int) (bb.limit() * decoder.averageCharsPerByte()));
        CoderResult cr = decoder.decode(bb, cb, endOfInput);
        cb.flip();

        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                emitter.onError(e);
                return new Result(null, false);
            }
        }

        ByteBuffer leftOver;
        if (bb.remaining() > 0) {
            leftOver = bb;
        } else {
            leftOver = null;
        }

        String string = cb.toString();
        if (!string.isEmpty())
            emitter.onNext(string);

        return new Result(leftOver, true);
    }

}
