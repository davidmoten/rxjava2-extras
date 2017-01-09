package com.github.davidmoten.rx2.buffertofile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.Flowables;

import io.reactivex.Flowable;

public class SerializersTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Serializers.class);
    }

    @Test
    public void testDataSerializer() {
        // Demonstrates DataSerializer usage
        DataSerializer<Integer> ds = new DataSerializer<Integer>() {

            @Override
            public void serialize(Integer t, DataOutput out) throws IOException {
                out.writeInt(t);
            }

            @Override
            public Integer deserialize(DataInput in) throws IOException {
                return in.readInt();
            }

            @Override
            public int sizeHint() {
                return 4;
            }
        };
        Flowable.just(4) //
                .compose(Flowables.onBackpressureBufferToFile() //
                        .serializer(ds)) //
                .test() //
                .awaitDone(5, TimeUnit.SECONDS) //
                .assertValue(4) //
                .assertComplete();
    }
    
    @Test
    public void testDataSerializerWithUnboundedCapacity() {
        // Demonstrates DataSerializer usage
        DataSerializer<Integer> ds = new DataSerializer<Integer>() {

            @Override
            public void serialize(Integer t, DataOutput out) throws IOException {
                out.writeInt(t);
            }

            @Override
            public Integer deserialize(DataInput in) throws IOException {
                return in.readInt();
            }

            @Override
            public int sizeHint() {
                return 0;
            }
        };
        Flowable.just(4) //
                .compose(Flowables.onBackpressureBufferToFile() //
                        .serializer(ds)) //
                .test() //
                .awaitDone(5, TimeUnit.SECONDS) //
                .assertValue(4) //
                .assertComplete();
    }
    
    @Test
    public void testUtf8() {
        Flowable.just("abc") //
        .compose(Flowables.onBackpressureBufferToFile() //
                .serializerUtf8()) //
        .test() //
        .awaitDone(5, TimeUnit.SECONDS) //
        .assertValue("abc") //
        .assertComplete();
    }

}
