package com.github.davidmoten.rx2.internal.flowable;

import java.util.List;

import org.junit.Test;

import com.github.davidmoten.rx2.FlowableTransformers;
import com.google.common.collect.Lists;

import io.reactivex.Flowable;
import io.reactivex.functions.BiPredicate;

public final class FlowableCollectWhileTest {

	private static final BiPredicate<List<Integer>, Integer> BUFFER_TWO = new BiPredicate<List<Integer>, Integer>() {

		@Override
		public boolean test(List<Integer> list, Integer t) throws Exception {
			return list.size() <= 1;
		}
	};

	@Test
	public void testEmpty() {
		Flowable.<Integer>empty() //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test() //
		        .assertNoValues() //
		        .assertComplete();
	}

	@Test
	public void testOne() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test() //
		        .assertValue(Lists.newArrayList(3)) //
		        .assertComplete();
	}

	@Test
	public void testTwo() {
		Flowable.just(3, 4) //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test() //
		        .assertValue(Lists.newArrayList(3, 4)) //
		        .assertComplete();
	}

	@Test
	public void testThree() {
		Flowable.just(3, 4, 5) //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test() //
		        .assertValues(Lists.newArrayList(3, 4), Lists.newArrayList(5)) //
		        .assertComplete();
	}
}
