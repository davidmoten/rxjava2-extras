package com.github.davidmoten.rx2;

import org.junit.Assert;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class BiFunctionsTest {

	@Test
	public void test() {
		Asserts.assertIsUtilityClass(BiFunctions.class);
	}

	@Test
	public void testConstant() throws Exception {
		Assert.assertEquals(1, (int) BiFunctions.constant(1).apply(new Object(), new Object()));
	}

	@Test
	public void testToNull() throws Exception {
		Assert.assertNull(BiFunctions.toNull().apply(new Object(), new Object()));
	}

}
