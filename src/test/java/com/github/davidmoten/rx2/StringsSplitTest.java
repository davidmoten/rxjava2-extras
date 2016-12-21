package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.subscribers.TestSubscriber;

public class StringsSplitTest {
    @Test
    public void testSplitLongPattern() {
        Iterator<String> iter = Strings.split(Flowable.just("asdfqw", "erasdf"), "qwer")
                .blockingIterable().iterator();
        assertTrue(iter.hasNext());
        assertEquals("asdf", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("asdf", iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSplitEmpty() {
        Flowable.<String> empty() //
                .compose(Strings.split("\n")) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }

    @Test(timeout = 5000)
    public void testSplitSimpleEmpty() {
        Flowable.<String> empty() //
                .compose(Strings.splitSimple("\n", 16)) //
                .test() //
                .assertNoValues() //
                .assertComplete();
    }

    @Test
    public void testSplitNormal() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.split(":")) //
                .test() //
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleNormal() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":")) //
                .test() //
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleNormalSmallBuffer() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":", 2)) //
                .test() //
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleBackpressure() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":")) //
                .test(0) //
                .assertNoValues() //
                .assertNotTerminated() //
                .requestMore(1) //
                .assertValue("boo") //
                .requestMore(1) //
                .assertValues("boo", "and") //
                .requestMore(1)//
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }

    @Test
    public void testSplitNormalWithPattern() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.split(Pattern.compile(":"))) //
                .test() //
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }

    @Test
    public void testSplitEmptyItemsAtBeginningMiddleAndEndProduceBlanks() {
        Flowable.just("::boo:an", "d:::you::") //
                .compose(Strings.split(":")) //
                .test() //
                .assertValues("", "", "boo", "and", "", "", "you", "", "") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleEmptyItemsAtBeginningMiddleAndEndProduceBlanks() {
        Flowable.just("::boo:an", "d:::you::") //
                .compose(Strings.splitSimple(":")) //
                .test() //
                .assertValues("", "", "boo", "and", "", "", "you", "", "") //
                .assertComplete();
    }

    @Test
    public void testSplitBlankProducesBlank() {
        Flowable.just("") //
                .compose(Strings.split(":")) //
                .test() //
                .assertValues("") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleBlankProducesBlank() {
        Flowable.just("") //
                .compose(Strings.splitSimple(":")) //
                .test() //
                .assertValues("") //
                .assertComplete();
    }

    @Test
    public void testSplitNoSeparatorProducesSingle() {
        Flowable.just("and") //
                .compose(Strings.split(":")) //
                .test() //
                .assertValues("and") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleNoSeparatorProducesSingle() {
        Flowable.just("and") //
                .compose(Strings.splitSimple(":")) //
                .test() //
                .assertValues("and") //
                .assertComplete();
    }

    @Test
    public void testSplitSeparatorOnlyProducesTwoBlanks() {
        Flowable.just(":") //
                .compose(Strings.split(":")) //
                .test() //
                .assertValues("", "") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleSeparatorOnlyProducesTwoBlanks() {
        Flowable.just(":") //
                .compose(Strings.splitSimple(":")) //
                .test() //
                .assertValues("", "") //
                .assertComplete();
    }

    @Test
    public void testSplitSimpleError() {
        Flowable.<String> error(new IOException()) //
                .compose(Strings.splitSimple(":")).test() //
                .assertNoValues() //
                .assertError(IOException.class);
    }

    @Test
    public void testSplitSimpleNormalCancelled() {
        TestSubscriber<String> ts = Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":")) //
                .test(2) //
                .assertValues("boo", "and").assertNotTerminated();
        ts.cancel();
        ts.assertValueCount(2);
        ts.assertNotTerminated();
    }

    @Test
    public void testSplitSimpleNormalCancelledEarly() {
        TestSubscriber<String> ts = Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":")) //
                .test(1) //
                .assertValues("boo").assertNotTerminated();
        ts.cancel();
        ts.assertValueCount(1);
        ts.assertNotTerminated();
    }

    @Test
    public void testSplitSimpleNormalCancelledAtBeginning() {
        TestSubscriber<String> ts = Flowable.just("boo:an", "d:you") //
                .compose(Strings.splitSimple(":")) //
                .test(0) //
                .assertNoValues() //
                .assertNotTerminated();
        ts.cancel();
        ts.requestMore(1);
        ts.assertNoValues();
        ts.assertNotTerminated();
    }

    @Test
    public void testSplitSimpleNormalCancelledAtBegginning() {
        try {
            List<Throwable> list = new CopyOnWriteArrayList<Throwable>();
            RxJavaPlugins.setErrorHandler(Consumers.addTo(list));
            Flowable.just("boo:an", "d:you") //
                    .compose(Strings.splitSimple(":")) //
                    .test(-5) //
                    .assertNoValues() //
                    .assertNotTerminated();
            assertEquals(1, list.size());
            assertTrue(list.get(0) instanceof IllegalArgumentException);
        } finally {
            RxJavaPlugins.reset();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitSimpleThrowsImmediatelyWhenBufferSizeIsZero() {
        Strings.splitSimple(":", 0).apply(Flowable.just("boo"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSplitSimpleThrowsImmediatelyWhenBufferSizeIsNegative() {
        Strings.splitSimple(":", -1).apply(Flowable.just("boo"));
    }

}
