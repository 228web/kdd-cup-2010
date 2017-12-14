package com.liyuncong.learn.kddcup2010.experiments;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import com.liyuncong.learn.kddcup2010.algorithm.LogisticRegression;
import com.liyuncong.learn.kddcup2010.data.Data;
import com.liyuncong.learn.kddcup2010.feature.BasicSparseFeatureTranformer;

/**
 * 
/usr/bin/spark-submit --driver-memory 16G --executor-memory 8G --num-executors 24 --executor-cores 4 --queue datateam_spark --conf spark.default.parallelism=96 --class com.liyuncong.learn.kddcup2010.experiments.LogisticRegressionForBasicSparseFeature kdd-cup-2010-jar-with-dependencies.jar > LogisticRegressionForBasicSparseFeature.log 2>&1 
 * @author liyuncong
 *
 */
public class LogisticRegressionForBasicSparseFeature {
	private LogisticRegressionForBasicSparseFeature(){}
	
	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName(LogisticRegressionForBasicSparseFeature.class.getSimpleName());
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		HiveContext hiveCtx = new HiveContext(jsc);
		
		DataFrame trainData = Data.getData(jsc, hiveCtx, "/tmp/kdd_cup_2010/algebra_2008_2009/algebra_2008_2009_train.txt"); 
		DataFrame predictData = Data.getData(jsc, hiveCtx, "/tmp/kdd_cup_2010/algebra_2008_2009/algebra_2008_2009_test.txt");
		BasicSparseFeatureTranformer basicSparseFeatureTranformer = new BasicSparseFeatureTranformer(hiveCtx, jsc, trainData, predictData);
		
		LogisticRegression logisticRegression = new LogisticRegression();
		logisticRegression.train(basicSparseFeatureTranformer.transormForSparkMllibTrain());
		
		JavaPairRDD<String, Double> predictResult = logisticRegression.predict(basicSparseFeatureTranformer.transormForSparkMllibPredict());
		
		LogisticRegression.persist("algebra_2008_2009_submission.txt", predictResult);
	}
}
