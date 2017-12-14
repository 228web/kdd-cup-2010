package com.liyuncong.learn.kddcup2010.feature;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicSparseFeatureTranformerTest {
	private JavaSparkContext jsc;
	
	@Before
	public void init() {
       SparkConf sparkConf = new SparkConf()
    		   .setAppName(BasicSparseFeatureTranformerTest.class.getSimpleName())
    		   .setMaster("local");
	   jsc = new JavaSparkContext(sparkConf);
	}

	@After
	public void destroy() {
		if (jsc != null) {
			jsc.close();
			jsc.stop();
		}
	}

	@Test
	public void testUniqueColumnValue() {
		
		List<Row> rows = new LinkedList<>();
		rows.add(RowFactory.create("li"));
		rows.add(RowFactory.create("li"));
		List<String> result = BasicSparseFeatureTranformer.uniqueColumnValue(jsc.parallelize(rows));
		assertEquals("li", result.get(0));
	}

}
