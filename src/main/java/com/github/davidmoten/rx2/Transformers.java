package com.github.davidmoten.rx2;

import java.nio.charset.CharsetDecoder;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import com.github.davidmoten.rx2.internal.flowable.FlowableTransformerStateMachine;
import com.github.davidmoten.rx2.internal.flowable.FlowableTransformerStringSplit;

import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;
import io.reactivex.functions.BiPredicate;
import io.reactivex.functions.Function3;

public class Transformers {
	
	private Transformers() {
		//prevent instantiation
	}

	public static FlowableTransformer<String, String> split(String pattern, BackpressureStrategy backpressureStrategy, int batchSize) {
		return FlowableTransformerStringSplit.split(pattern, null, backpressureStrategy, batchSize);
	}

	public static FlowableTransformer<String, String> split(Pattern pattern, BackpressureStrategy backpressureStrategy, int batchSize) {
		return FlowableTransformerStringSplit.split(null, pattern, backpressureStrategy, batchSize);
	}

	public static FlowableTransformer<byte[], String> decode(CharsetDecoder decoder) {
		// TODO Auto-generated method stub
		return null;
	}

	public static <State, In, Out> FlowableTransformer<In, Out> stateMachine(Callable<? extends State> initialState,
			Function3<? super State, ? super In, ? super FlowableEmitter<Out>, ? extends State> transition,
			BiPredicate<? super State, ? super FlowableEmitter<Out>> completion,
			BackpressureStrategy backpressureStrategy, int initialRequest) {
		return FlowableTransformerStateMachine.create(initialState, transition, completion, backpressureStrategy,
				initialRequest);
	}

}
