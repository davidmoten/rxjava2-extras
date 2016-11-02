package com.github.davidmoten.rx2;

import java.nio.charset.CharsetDecoder;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.github.davidmoten.rx2.StateMachine;
import com.github.davidmoten.rx2.internal.flowable.TransformerDecode;
import com.github.davidmoten.rx2.internal.flowable.TransformerStateMachine;
import com.github.davidmoten.rx2.internal.flowable.TransformerStringSplit;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public class Transformers {
	
	public static final int DEFAULT_INITIAL_BATCH = 1;

	private Transformers() {
		// prevent instantiation
	}

	public static FlowableTransformer<String, String> split(String pattern, BackpressureStrategy backpressureStrategy,
			int requestBatchSize) {
		return TransformerStringSplit.split(pattern, null, backpressureStrategy, requestBatchSize);
	}

	public static FlowableTransformer<String, String> split(Pattern pattern, BackpressureStrategy backpressureStrategy,
			int batchSize) {
		return TransformerStringSplit.split(null, pattern, backpressureStrategy, batchSize);
	}

	public static FlowableTransformer<byte[], String> decode(CharsetDecoder decoder) {
		return TransformerDecode.decode(decoder, BackpressureStrategy.BUFFER, DEFAULT_INITIAL_BATCH);
	}
	
	public static FlowableTransformer<byte[], String> decode(CharsetDecoder decoder, BackpressureStrategy backpressureStrategy, int requestBatchSize) {
		return TransformerDecode.decode(decoder, BackpressureStrategy.BUFFER, requestBatchSize);
	}

	public static <State, In, Out> FlowableTransformer<In, Out> stateMachine(Callable<? extends State> initialState,
			Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
			BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
			BackpressureStrategy backpressureStrategy, int requestBatchSize) {
		return TransformerStateMachine.create(initialState, transition, completion, backpressureStrategy,
				requestBatchSize);
	}
	
	public static StateMachine.Builder stateMachine() {
        return StateMachine.builder();
    }

}
