package com.github.davidmoten.rx2.internal.flowable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Test;

import com.github.davidmoten.rx2.BiFunctions;
import com.github.davidmoten.rx2.BiPredicates;
import com.github.davidmoten.rx2.Callables;
import com.github.davidmoten.rx2.Consumers;
import com.github.davidmoten.rx2.FlowableTransformers;
import com.github.davidmoten.rx2.exceptions.ThrowingException;
import com.github.davidmoten.rx2.flowable.Burst;
import com.google.common.collect.Lists;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.BiPredicate;
import io.reactivex.plugins.RxJavaPlugins;

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

	@SuppressWarnings("unchecked")
	@Test
	public void testThree() {
		Flowable.just(3, 4, 5) //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test() //
		        .assertValues(Lists.newArrayList(3, 4), Lists.newArrayList(5)) //
		        .assertComplete();
	}

	@Test
	public void testFactoryReturnsNullShouldEmitNPE() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                collectWhile(Callables.<List<Integer>>toNull(), BiFunctions.constant(new ArrayList<Integer>()),
		                        BUFFER_TWO)) //
		        .test() //
		        .assertNoValues() //
		        .assertError(NullPointerException.class);
	}

	@Test
	public void testAddReturnsNullShouldEmitNPE() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                collectWhile(Callables.<List<Integer>>toNull(),
		                        BiFunctions.<List<Integer>, Integer, List<Integer>>toNull(), BUFFER_TWO)) //
		        .test() //
		        .assertNoValues() //
		        .assertError(NullPointerException.class);
	}

	@Test
	public void testAddReturnsNull() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                collectWhile( //
		                        Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()),
		                        BiFunctions.<List<Integer>, Integer, List<Integer>>toNull(), //
		                        BUFFER_TWO)) //
		        .test() //
		        .assertNoValues() //
		        .assertError(NullPointerException.class);
	}

	@Test
	public void testAddThrows() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                collectWhile( //
		                        Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()),
		                        BiFunctions.<List<Integer>, Integer, List<Integer>>throwing(), //
		                        BUFFER_TWO)) //
		        .test() //
		        .assertNoValues() //
		        .assertError(ThrowingException.class);
	}

	@Test
	public void testConditionThrows() {
		Flowable.just(3) //
		        .compose(FlowableTransformers. //
		                collectWhile( //
		                        Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
		                        BiPredicates.throwing())) //
		        .test() //
		        .assertNoValues() //
		        .assertError(ThrowingException.class);
	}

	@Test
	public void testDoesNotEmitAfterErrorInOnNextIfUpstreamDoesNotHonourCancellationImmediately() {
		Burst.items(1, 2).create() //
		        .compose(FlowableTransformers. //
		                collectWhile( //
		                        Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
		                        BiPredicates.throwing())) //
		        .test() //
		        .assertNoValues() //
		        .assertError(ThrowingException.class);
	}

	@Test
	public void testDoesNotTwoErrorsIfUpstreamDoesNotHonourCancellationImmediately() {
		try {
			List<Throwable> list = new CopyOnWriteArrayList<Throwable>();
			RxJavaPlugins.setErrorHandler(Consumers.addTo(list));
			Burst.items(1).error(new ThrowingException())//
			        .compose(FlowableTransformers. //
			                collectWhile( //
			                        Callables.<List<Integer>>constant(Lists.<Integer>newArrayList()), ADD, //
			                        BiPredicates.throwing())) //
			        .test() //
			        .assertNoValues() //
			        .assertError(ThrowingException.class);
			assertEquals(1, list.size());
			assertTrue(list.get(0) instanceof ThrowingException);
		} finally {
			RxJavaPlugins.reset();
		}
	}

	@Test
	public void testBackpressure() {
		Flowable.just(3, 4, 5) //
		        .compose(FlowableTransformers. //
		                toListWhile(BUFFER_TWO)) //
		        .test(1) //
		        .assertValue(Lists.newArrayList(3, 4)) //
		        .assertNotTerminated();
	}
	
	private static final BiFunction<List<Integer>, Integer, List<Integer>> ADD = new BiFunction<List<Integer>, Integer, List<Integer>>() {

		@Override
		public List<Integer> apply(List<Integer> list, Integer t) throws Exception {
			list.add(t);
			return list;
		}
	};

}
