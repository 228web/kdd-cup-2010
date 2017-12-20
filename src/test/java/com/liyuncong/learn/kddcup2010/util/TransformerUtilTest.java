package com.liyuncong.learn.kddcup2010.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Test;

public class TransformerUtilTest {

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTransform() {
		fail("Not yet implemented");
	}

	@Test
	public void testStrintToWritable() throws IOException, InterruptedException {
		String csv = "stu_de2777346f,Unit CTA1_01, Section CTA1_01-3,REAL20B,R4C2,Using simple numbers~~Find Y  any form~~Using large numbers~~Find Y  negative slope,1.0,6.0,1";
		System.out.println(TransformerUtil.strintToWritable(csv.substring(0, csv.lastIndexOf(','))).size());
	}

	@Test
	public void testWritablesToString() {
		fail("Not yet implemented");
	}

}
