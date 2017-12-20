package com.liyuncong.learn.kddcup2010;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用于测试不确定api功能
 * @author liyuncong
 *
 */
public class ApiTest {
	@Test
	public void test1() {
		String string = "Row	Anon Student Id	Problem Hierarchy	Problem Name	Problem View	Step Name	Step Start Time	First Transaction Time	Correct Transaction Time	Step End Time	Step Duration (sec)	Correct Step Duration (sec)	Error Step Duration (sec)	Correct First Attempt	Incorrects	Hints	Corrects	KC(Default)	Opportunity(Default)";
		String[] parts = string.split("\t");
		System.out.println("length:" + parts.length);
		for (String string2 : parts) {
			System.out.println(string2);
		}
	}
	@Test
	public void test2() {
		String string = "9	stu_de2777346f	Unit CTA1_01, Section CTA1_01-3	REAL20B	1	R6C2	2008-09-19 13:30:46.0	2008-09-19 13:30:46.0	2008-09-19 13:30:46.0	2008-09-19 13:30:46.0	0	0		1	0	0	1	Using simple numbers~~Find Y, any form~~Using large numbers~~Find Y, negative slope	4~~3~~4~~3	Using simple numbers-1~~Using large numbers-1~~Find Y, negative slope-1	4~~4~~3	CALCULATED-VALUE-MX+B-GIVEN-X-SLOPE-NEGATIVE	2";
		String[] parts = string.split("\t");
		System.out.println("length:" + parts.length);
		for (String string2 : parts) {
			System.out.println(string2);
		}
	}
	
	@Test
	public void test3() {
		System.out.println(Integer.parseInt(null));
	}
	
	@Test
	public void test4() {
		List<String> list = new LinkedList<>();
		list.add("li");
		list.add("yuncong");
		System.out.println(StringUtils.join(list, ","));
	}
	
	@Test
	public void test5() {
		String csv = "li,yun,cong";
		String[] values = csv.split(",");
		Row row = RowFactory.create(values);
		System.out.println(row.size());
		System.out.println(row);
	}
	
	@Test
	public void test6() {
		StructType tagCsvSchame = new StructType(new StructField[]{
				  new StructField("anonStudentId", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("unitName", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("sectionName", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("problemName", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("stepName", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("kcDefault", DataTypes.StringType, false, Metadata.empty()),
				  new StructField("problemView", DataTypes.DoubleType, false, Metadata.empty()),
				  new StructField("opportunityDefault", DataTypes.DoubleType, false, Metadata.empty())

		});
		for(int i = 0; i < tagCsvSchame.size(); i++) {
			StructField structField = tagCsvSchame.apply(i);
			System.out.println(structField.name() + "=" + structField.dataType() + ":" + structField.dataType().equals(DataTypes.DoubleType));
		}
	}
	
	@Test
	public void test7() {
		System.out.println("36~~33~~33".replaceAll("~~", ","));
	}
	
	@Test
	public void test8() {
		System.out.println(Double.valueOf(""));
	}
	
	@Test
	public void test9() {
		String[] array = null;
		System.out.println(array[0]);
	}
	
	@Test
	public void test10() {
		String string = "";
		System.out.println(string.split(":")[0]);
	}
	
	@Test
	public void test11() {
		Logger logger = LoggerFactory.getLogger("test");
		String string = null;
		logger.info("{}", string);
	}
	
	@Test
	public void test12() {
		Logger logger = LoggerFactory.getLogger("test");
		Map<String, String> map = new HashMap<>();
		map.put("li", "wang");
		map.put("l", "zhang");
		logger.info("{}", map);
	}
}
