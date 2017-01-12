package com.github.davidmoten.rx2;

import io.reactivex.Flowable;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by erzfn on 12/1/17.
 */
public class FlowableSerializedTest {
    @Test
    public void testSerializeAndDeserializeOfNonEmptyStream() {
        File file = new File("target/temp1");
        file.delete();
        Flowable<Integer> source = Flowable.just(1, 2, 3);
        FlowableSerialized.write(source, file).subscribe();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);
        List<Integer> list = FlowableSerialized.<Integer> read(file).toList().blockingGet();
        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    @Test
    public void testSerializeAndDeserializeOfNonEmptyStreamWithSmallBuffer() {
        File file = new File("target/temp2");
        file.delete();
        Flowable<Integer> source = Flowable.just(1, 2, 3);
        FlowableSerialized.write(source, file, false, 1).subscribe();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);
        List<Integer> list = FlowableSerialized.<Integer> read(file, 1).toList().blockingGet();
        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    @Test
    public void testSerializeAndDeserializeOfEmptyStream() {
        File file = new File("target/temp3");
        file.delete();
        Flowable<Integer> source = Flowable.empty();
        FlowableSerialized.write(source, file).subscribe();
        assertTrue(file.exists());
        List<Integer> list = FlowableSerialized.<Integer> read(file).toList().blockingGet();
        assertTrue(list.isEmpty());
    }

    @Test
    public void testSerializeAndDeserializeOfNonEmptyStreamUsingKryo() {
        File file = new File("target/temp4");
        file.delete();
        Flowable<Integer> source = Flowable.just(1, 2, 3);
        FlowableSerialized.kryo().write(source, file).subscribe();
        assertTrue(file.exists());
        assertTrue(file.length() > 0);
        List<Integer> list = FlowableSerialized.kryo().read(Integer.class, file).toList().blockingGet();
        assertEquals(Arrays.asList(1, 2, 3), list);
    }

    @Test
    public void testSerializeAndDeserializeOfEmptyStreamUsingKryo() {
        File file = new File("target/temp5");
        file.delete();
        Flowable<Integer> source = Flowable.empty();
        FlowableSerialized.kryo().write(source, file).subscribe();
        assertTrue(file.exists());
        List<Integer> list = FlowableSerialized.kryo().read(Integer.class, file).toList().blockingGet();
        assertTrue(list.isEmpty());
    }

    @Test
    public void testSerializeAndDeserializeOfPersonStreamUsingKryo() {
        File file = new File("target/temp6");
        file.delete();
        Flowable<Person> source = Flowable.just(new Person("fred", 24), new Person("jane", 32));
        FlowableSerialized.kryo().write(source, file).subscribe();
        assertTrue(file.exists());
        List<Person> list = FlowableSerialized.kryo().read(Person.class, file).toList().blockingGet();
        assertEquals(2, list.size());
        assertEquals("fred", list.get(0).name);
        assertEquals(24, list.get(0).age);
        assertEquals("jane", list.get(1).name);
        assertEquals(32, list.get(1).age);
    }

    static class Person {
        // Note Person class doesn't need to implement Serializable to be
        // serialized by kryo

        final String name;
        final int age;

        Person() {
            // requires no-arg constructor to be serialized by kryo
            this("", 0);
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

    }
}