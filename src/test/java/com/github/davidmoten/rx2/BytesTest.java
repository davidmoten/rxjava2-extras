package com.github.davidmoten.rx2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.util.ZippedEntry;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import io.reactivex.functions.Predicate;

public class BytesTest {

    @Test
    public void testUnzip() {
        List<String> list = Bytes.unzip(new File("src/test/resources/test.zip"))
                .concatMap(new Function<ZippedEntry, Flowable<String>>() {

                    @Override
                    public Flowable<String> apply(ZippedEntry entry) {
                        return Flowable.just(entry.getName()).concatWith(Strings.from(entry.getInputStream()));
                    }
                }).toList().blockingGet();
        assertEquals(Arrays.asList("document1.txt", "hello there", "document2.txt", "how are you going?"), list);
    }

    @Test
    public void testUnzipPartial() {
        InputStream is = BytesTest.class.getResourceAsStream("/test.zip");
        assertNotNull(is);
        List<String> list = Bytes.unzip(is).concatMap(new Function<ZippedEntry, Flowable<String>>() {

            @Override
            public Flowable<String> apply(ZippedEntry entry) {
                try {
                    return Flowable.just((char) entry.getInputStream().read() + "");
                } catch (IOException e) {
                    return Flowable.error(e);
                }
            }
        }).toList().blockingGet();
        assertEquals(Arrays.asList("h", "h"), list);
    }

    @Test
    public void testUnzipCheckEntryFields() {
        Bytes.unzip(new File("src/test/resources/test.zip")) //
                .filter(new Predicate<ZippedEntry>() {

                    @Override
                    public boolean test(ZippedEntry entry) {
                        return entry.getName().equals("document2.txt");
                    }
                }).doOnNext(new Consumer<ZippedEntry>() {
                    @Override
                    public void accept(ZippedEntry e) throws Exception {
                        assertEquals(1091476648, e.getCrc());
                        assertNull(e.getComment());
                        assertEquals(18, e.getCompressedSize());
                        assertEquals(18, e.getSize());
                        assertEquals(0, e.getMethod());
                        assertEquals("document2.txt", e.getName());
                        assertTrue(e.getTime() > 0);
                        assertNotNull(e.getExtra());
                    }
                }).test().assertNoErrors().assertComplete();
    }

    @Test
    public void testUnzipExtractSpecificFile() {
        List<String> list = Bytes.unzip(new File("src/test/resources/test.zip")).filter(new Predicate<ZippedEntry>() {

            @Override
            public boolean test(ZippedEntry entry) {
                return entry.getName().equals("document2.txt");
            }
        }).concatMap(new Function<ZippedEntry, Flowable<String>>() {

            @Override
            public Flowable<String> apply(ZippedEntry entry) {
                return Strings.from(entry.getInputStream());
            }
        }).toList().blockingGet();
        assertEquals(Arrays.asList("how are you going?"), list);
    }

    @Test
    public void isUtilClass() {
        Asserts.assertIsUtilityClass(Bytes.class);
    }

    @Test
    public void testBytesFromFileLowBufferSize() throws IOException {
        File file = new File("target/testFromFile");
        file.delete();
        FileOutputStream out = new FileOutputStream(file);
        out.write("abcdefg".getBytes());
        out.close();
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Bytes //
                .from(file, 4) //
                .doOnNext(new Consumer<byte[]>() {
                    @Override
                    public void accept(byte[] b) {
                        try {
                            bytes.write(b);
                        } catch (IOException e) {
                            throw new RuntimeException();
                        }
                    }
                }).subscribe();
        bytes.close();
        assertArrayEquals("abcdefg".getBytes(), bytes.toByteArray());
    }

    @Test
    public void testBytesFromFileWhenDoesNotExist() {
        Bytes.from(new File("target/" + UUID.randomUUID())) //
                .test() //
                .assertError(FileNotFoundException.class);
    }

    @Test
    public void testBytesFromInputStreamDefaultBufferSize() {
        String s = "hello there";
        Bytes.from(new ByteArrayInputStream(s.getBytes(Strings.UTF_8))) //
                .to(Bytes.collect()) //
                .map(new Function<byte[], String>() {

                    @Override
                    public String apply(byte[] b) throws Exception {
                        return new String(b, Strings.UTF_8);
                    }
                }) //
                .test().assertValue("hello there") //
                .assertComplete();
    }

    @Test
    public void testBytesFromFileDefaultBufferSize() throws IOException {
        File file = new File("target/testFromFile");
        file.delete();
        FileOutputStream out = new FileOutputStream(file);
        out.write("abcdefg".getBytes());
        out.close();
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Bytes //
                .from(file) //
                .doOnNext(new Consumer<byte[]>() {
                    @Override
                    public void accept(byte[] b) {
                        try {
                            bytes.write(b);
                        } catch (IOException e) {
                            throw new RuntimeException();
                        }
                    }
                }).subscribe();
        bytes.close();
        assertArrayEquals("abcdefg".getBytes(), bytes.toByteArray());
    }

    @Test
    public void testCollect() {
        byte[] a = { 1, 2, 3 };
        byte[] b = { 4, 5, 6 };
        byte[] result = Flowable //
                .just(a, b) //
                .to(Bytes.collect()).blockingGet();
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3, 4, 5, 6 }, result));
    }

    @Test
    public void testCollectWithEmpty() {
        byte[] a = { 1, 2, 3 };
        byte[] b = {};
        byte[] result = Flowable //
                .just(a, b) //
                .to(Bytes.collect()).blockingGet();
        assertTrue(Arrays.equals(new byte[] { 1, 2, 3 }, result));
    }

    @Test
    public void testCollectWithEmpties() {
        byte[] a = {};
        byte[] b = {};
        byte[] result = Flowable //
                .just(a, b) //
                .to(Bytes.collect()).blockingGet();
        assertTrue(Arrays.equals(new byte[] {}, result));
    }

}
