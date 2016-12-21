package com.github.davidmoten.rx2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.junit.Test;

import io.reactivex.Flowable;

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
    public void testSplitEmpty2() {
        Flowable.<String> empty() //
                .compose(Strings.split2("\n", 16)) //
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
    public void testSplitNormal2() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.split2(":")) //
                .test() //
                .assertValues("boo", "and", "you") //
                .assertComplete();
    }
    
    @Test
    public void testSplitNormalSmallBuffer2() {
        Flowable.just("boo:an", "d:you") //
                .compose(Strings.split2(":",2)) //
                .test() //
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
    public void testSplitEmptyItemsAtBeginningMiddleAndEndProduceBlanks2() {
        Flowable.just("::boo:an", "d:::you::") //
                .compose(Strings.split2(":")) //
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
    public void testSplitBlankProducesBlank2() {
        Flowable.just("") //
                .compose(Strings.split2(":")) //
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
    public void testSplitNoSeparatorProducesSingle2() {
        Flowable.just("and") //
                .compose(Strings.split2(":")) //
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
    public void testSplitSeparatorOnlyProducesTwoBlanks2() {
        Flowable.just(":") //
                .compose(Strings.split2(":")) //
                .test() //
                .assertValues("", "") //
                .assertComplete();
    }

}
