package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.StateMachine.Completion;
import com.github.davidmoten.rx2.StateMachine.CompletionAlwaysTrueHolder;
import com.github.davidmoten.rx2.StateMachine.Transition;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableTransformer;

public class StateMachineTest {

    @Test
    public void testIsUtilityClass() {
        Asserts.assertIsUtilityClass(StateMachine.class);
    }

    @Test
    public void testIsUtilityClassCompletionAlwaysTrueHolder() {
        Asserts.assertIsUtilityClass(CompletionAlwaysTrueHolder.class);
    }

    @Test
    public void testBuilder() {
        FlowableTransformer<Integer, String> collectIntoStringsOfMinLength3 = Transformers
                .stateMachine() //
                .initialState("") //
                .transition(new Transition<String, Integer, String>() {
                    @Override
                    public String apply(String state, Integer value,
                            FlowableEmitter<String> emitter) {
                        String state2 = state + value;
                        if (state2.length() >= 3) {
                            emitter.onNext(state2.substring(0, 3));
                            return state2.substring(3);
                        } else {
                            return state2;
                        }
                    }

                }) //
                .completion(new Completion<String, String>() {
                    @Override
                    public boolean test(String state, FlowableEmitter<String> emitter) {
                        emitter.onNext(state);
                        return true;
                    }
                }) //
                .backpressureStrategy(BackpressureStrategy.BUFFER) //
                .requestBatchSize(128) //
                .build();

        List<String> list = Flowable.range(1, 13).compose(collectIntoStringsOfMinLength3).toList()
                .blockingGet();
        assertEquals(Arrays.asList("123", "456", "789", "101", "112", "13"), list);

    }

}
