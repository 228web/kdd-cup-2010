package com.liyuncong.learn.kddcup2010.data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 
 * @author liyuncong
 *
 */
public class Data {
	private Data(){}
	
	public static DataFrame getData(JavaSparkContext jsc, HiveContext hiveCtx, 
			String dataPath) {
		JavaRDD<OneStepData> ratingRdd = jsc.textFile(dataPath).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = -2133286941760763560L;

			@Override
			public Boolean call(String line) throws Exception {
				return !line.startsWith("Row");
			}
		}).map(new Function<String, OneStepData>() {

			private static final long serialVersionUID = 5092259588085890853L;

			@Override
			public OneStepData call(String line) throws Exception {
				return OneStepData.parse(line);
			}
		});
		return hiveCtx.createDataFrame(ratingRdd, OneStepData.class)
				.selectExpr(OneStepData.SORTED_FIELD.split(","));
	}
}
