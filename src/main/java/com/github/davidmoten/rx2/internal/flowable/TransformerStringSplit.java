package com.github.davidmoten.rx2.internal.flowable;

import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.github.davidmoten.rx2.Callables;
import com.github.davidmoten.rx2.FlowableTransformers;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public final class TransformerStringSplit {

    private TransformerStringSplit() {
        // prevent instantiation
    }

    public static <T> FlowableTransformer<String, String> split(final String pattern, final Pattern compiledPattern,
            final BackpressureStrategy backpressureStrategy, int batchSize) {
        Callable<String> initialState = Callables.constant(null);
        Function3<String, String, FlowableEmitter<String>, String> transition = new Function3<String, String, FlowableEmitter<String>, String>() {

            @Override
            public String apply(String leftOver, String s, FlowableEmitter<String> emitter) {
                // prepend leftover to the string before splitting
                if (leftOver != null) {
                    s = leftOver + s;
                }

                String[] parts;
                if (compiledPattern != null) {
                    parts = compiledPattern.split(s, -1);
                } else {
                    parts = s.split(pattern, -1);
                }

                // can emit all parts except the last part because it hasn't
                // been terminated by the pattern/end-of-stream yet
                for (int i = 0; i < parts.length - 1; i++) {
                    if (emitter.isCancelled()) {
                        // won't be used so can return null
                        return null;
                    }
                    emitter.onNext(parts[i]);
                }

                // we have to assign the last part as leftOver because we
                // don't know if it has been terminated yet
                return parts[parts.length - 1];
            }
        };

        BiPredicate<String, FlowableEmitter<String>> completion = new BiPredicate<String, FlowableEmitter<String>>() {

            @Override
            public boolean test(String leftOver, FlowableEmitter<String> emitter) {
                if (leftOver != null && !emitter.isCancelled())
                    emitter.onNext(leftOver);
                // TODO is this check needed?
                if (!emitter.isCancelled())
                    emitter.onComplete();
                return true;
            }
        };
        return FlowableTransformers.stateMachine(initialState, transition, completion, backpressureStrategy, batchSize);
    }

}
