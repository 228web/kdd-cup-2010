package com.liyuncong.learn.kddcup2010.data;

import static org.junit.Assert.*;

import org.junit.Test;

public class OneStepDataTest {

	@Test
	public void testParse() throws NumberFormatException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		String line = "717	0BrbPbwCMz	Unit CTA1_10, Section CTA1_10-5	DIST05_SP	1	R7C1												Using small numbers~~Find X, positive slope~~Using difficult numbers	36~~33~~33";
		OneStepData oneStepData = OneStepData.parse(line);
		System.out.println(oneStepData);
	}

}
