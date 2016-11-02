package com.github.davidmoten.rx2;

import static com.github.davidmoten.rx2.Strings.decode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.functions.BiFunction;


public class StringsTest {

    @Test
    public void testMultibyteSpanningTwoBuffers() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2 },
                new byte[] { (byte) 0xa1 });
        String out = decode(src, "UTF-8").blockingSingle();

        assertEquals("\u00A1", out);
    }

    @Test
    public void testMalformedAtTheEndReplace() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2 });
        String out = decode(src, "UTF-8").blockingSingle();

        // REPLACEMENT CHARACTER
        assertEquals("\uFFFD", out);
    }

    @Test
    public void testMalformedInTheMiddleReplace() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2, 65 });
        String out = decode(src, "UTF-8").blockingSingle();

        // REPLACEMENT CHARACTER
        assertEquals("\uFFFDA", out);
    }

    @Test(expected = RuntimeException.class)
    public void testMalformedAtTheEndReport() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2 });
        CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
        decode(src, charsetDecoder).blockingSingle();
    }

    @Test(expected = RuntimeException.class)
    public void testMalformedInTheMiddleReport() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2, 65 });
        CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
        decode(src, charsetDecoder).blockingSingle();
    }

    @Test
    public void testPropagateError() {
        Flowable<byte[]> src = Flowable.just(new byte[] { 65 });
        Flowable<byte[]> err = Flowable.error(new IOException());
        CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
        try {
            decode(Flowable.concat(src, err), charsetDecoder).toList().blockingGet();
            fail();
        } catch (RuntimeException e) {
            assertEquals(IOException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testPropagateErrorInTheMiddleOfMultibyte() {
        Flowable<byte[]> src = Flowable.just(new byte[] { (byte) 0xc2 });
        Flowable<byte[]> err = Flowable.error(new IOException());
        CharsetDecoder charsetDecoder = Charset.forName("UTF-8").newDecoder();
        try {
            decode(Flowable.concat(src, err), charsetDecoder).toList().blockingGet();
            fail();
        } catch (RuntimeException e) {
            assertEquals(IOException.class, e.getCause().getClass());
        }
    }
    
    @Test
    public void testFromClasspath() {
    	String expected = "hello world\nincoming message";
    	assertEquals(expected, Strings.fromClasspath("/test2.txt").reduce(new BiFunction<String, String, String>() {
			@Override
			public String apply(String a, String b) {
				return a+b;
			}
		}).blockingGet());
    }
}
