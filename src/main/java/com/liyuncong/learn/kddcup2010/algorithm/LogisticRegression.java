package com.liyuncong.learn.kddcup2010.algorithm;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.optimization.L1Updater;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class LogisticRegression {
	private LogisticRegressionModel model;
	public void train(JavaRDD<LabeledPoint> trainExamples) {
		LogisticRegressionWithLBFGS logisticRegressionWithLBFGS = new LogisticRegressionWithLBFGS();
		logisticRegressionWithLBFGS.setNumClasses(2);
		logisticRegressionWithLBFGS.optimizer().setUpdater(new L1Updater());
		model = logisticRegressionWithLBFGS.run(trainExamples.rdd());
	}
	
	public JavaPairRDD<String, Double> predict(JavaPairRDD<String, Vector> predictExamples) {
		final LogisticRegressionModel predictModel = model;
		return predictExamples.mapToPair(new PairFunction<Tuple2<String,Vector>, String, Double>() {

			private static final long serialVersionUID = 4711866762325937766L;

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Vector> idAndVector)
					throws Exception {
				return new Tuple2<String, Double>(idAndVector._1, predictModel.predict(idAndVector._2));
			}
		});
	}
	
	public static void persist(String path, JavaPairRDD<String, Double> idAndPredict) throws IOException {
		Map<Integer, Double> predictResult = idAndPredict.mapToPair(new PairFunction<Tuple2<String,Double>, Integer, Double>() {

			private static final long serialVersionUID = -4813451410899944353L;

			@Override
			public Tuple2<Integer, Double> call(Tuple2<String, Double> t) throws Exception {
				return new Tuple2<Integer, Double>(Integer.parseInt(t._1), t._2);
			}
		}).collectAsMap();
		TreeMap<Integer, Double> sortedPredictResult = new TreeMap<>(predictResult);
		List<String> lines = new LinkedList<>();
		lines.add("Row	Correct First Attempt");
		for(Entry<Integer, Double> entry : sortedPredictResult.entrySet()) {
			lines.add(entry.getKey() + "\t" + entry.getValue());
		}
		Path path2 = Paths.get(path);
		Files.write(path2, lines, Charset.forName("utf-8"), StandardOpenOption.CREATE_NEW);
	}
}
