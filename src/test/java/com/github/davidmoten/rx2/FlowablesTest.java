package com.github.davidmoten.rx2;

import org.junit.Ignore;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;
import com.github.davidmoten.rx2.exceptions.ThrowingException;

import io.reactivex.Flowable;
import io.reactivex.exceptions.MissingBackpressureException;
import io.reactivex.functions.BiFunction;

public class FlowablesTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Flowables.class);
    }

    
}
