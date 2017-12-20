package com.liyuncong.learn.kddcup2010.feature;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.StringSplit;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.analysis.DataAnalysis;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.transform.normalize.Normalize;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.AnalyzeSpark;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liyuncong.learn.kddcup2010.util.TransformerUtil;

import scala.Tuple2;

/**
 * 
 * @author liyuncong
 *
 */
public class FeatureCombinationTranformer implements Serializable{
	private static final long serialVersionUID = -499548205482995752L;
	private transient Logger logger = LoggerFactory.getLogger(FeatureCombinationTranformer.class);
	private static List<String> selectedFeatureColumns = new LinkedList<>();
	private static final int opportunityDefaultIndex = 7;
	private transient HiveContext hiveContext;
	private transient JavaSparkContext sparkContext;
	private transient DataFrame trainData;
	private transient DataFrame predictData;
	private transient TransformProcess featureTransformProcess;

	static {
		selectedFeatureColumns.add("anonStudentId");
		selectedFeatureColumns.add("unitName");
		selectedFeatureColumns.add("sectionName");
		selectedFeatureColumns.add("problemName");
		selectedFeatureColumns.add("stepName");
		selectedFeatureColumns.add("kcDefault");
		selectedFeatureColumns.add("problemView");
		selectedFeatureColumns.add("opportunityDefault");
		selectedFeatureColumns.add("concat(anonStudentId, '_', unitName)");
		selectedFeatureColumns.add("concat(unitName, '_', sectionName)");
		selectedFeatureColumns.add("concat(sectionName, '_', problemName)");
		selectedFeatureColumns.add("concat(problemName, '_', stepName)");
		selectedFeatureColumns.add("concat(anonStudentId, '_', unitName, '_', sectionName)");
		selectedFeatureColumns.add("concat(unitName, '_', sectionName, '_', problemName)");
		selectedFeatureColumns.add("concat(sectionName, '_', problemName, '_', stepName)");
		selectedFeatureColumns.add("concat(anonStudentId, '_', unitName, '_', sectionName, '_', problemName)");
		selectedFeatureColumns.add("concat(unitName, '_', sectionName, '_', problemName, '_', stepName)");

	}
	
	public FeatureCombinationTranformer(HiveContext hiveContext, JavaSparkContext sparkContext, DataFrame trainData,
			DataFrame predictData) {
		super();
		this.hiveContext = hiveContext;
		this.sparkContext = sparkContext;
		this.trainData = trainData;
		this.predictData = predictData;
		logger.info("trainData count:{}", trainData.count());
		logger.info("predictData count:{}", predictData.count());
		init();
	}

	private void init() {
		DataFrame selectedData = trainData.selectExpr(selectedFeatureColumns.toArray(new String[0]));
		Row firstSelectedData = selectedData.first();
		logger.info("firstSelectedData:{}", firstSelectedData);
		
		// dataSchema
		Schema dataSchema = getDataSchema(selectedData);
		logger.info("dataSchema:{}", dataSchema.toJson());
		
		// TransformProcess
		JavaRDD<String> tagCsvs = selectedData.toJavaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 4004418294052114465L;

			@Override
			public String call(Row row) throws Exception {
				return rowToOriginalFeatureCsv(row, opportunityDefaultIndex);
			}
		});
		RecordReader recordReader = new CSVRecordReader();
		JavaRDD<List<Writable>> parsedTagCsvs = tagCsvs.map(new StringToWritablesFunction(recordReader));
		int maxHistogramBuckets = 10;
		DataAnalysis dataAnalysis = AnalyzeSpark.analyze(dataSchema, parsedTagCsvs, maxHistogramBuckets);
		this.featureTransformProcess = getDataTransformProcess(dataSchema, dataAnalysis, selectedData.schema());
		logger.info("featureTransformProcess:{}", featureTransformProcess.toJson());
	}

	private static String sumOpportunity(String opportunity) {
		Objects.requireNonNull(opportunity);
		double sum = 0;
		for(String value : opportunity.split("~~")) {
			sum += Double.valueOf(value);
		}
		return String.valueOf(sum);
	}
	
	/**
	 * 
	 * @param orginalData
	 * @param hiveContext
	 * @param train
	 * @return key:特征 value:train为true时为类别，false时为row
	 */
	public JavaPairRDD<List<Writable>, String> transform(final boolean train) {
		DataFrame data;
		if (train) {
			data = trainData;
		} else {
			data = predictData;
		}
		
		List<String> selectedColumns = new LinkedList<>();
		selectedColumns.addAll(selectedFeatureColumns);
		if (train) {
			selectedColumns.add("correctFirstAttempt");
		} else {
			selectedColumns.add("row");
		}
		
		DataFrame selectedData = data.selectExpr(selectedColumns.toArray(new String[0]));
		
		JavaRDD<String> tagCsvs = selectedData.toJavaRDD().map(new Function<Row, String>() {

			private static final long serialVersionUID = 4004418294052114465L;

			@Override
			public String call(Row row) throws Exception {
				return rowToOriginalFeatureCsv(row, opportunityDefaultIndex);
			}
		});
		
		final Broadcast<String> transformProcessJsonBc = sparkContext.broadcast(featureTransformProcess.toJson());
		
		return tagCsvs.mapToPair(new PairFunction<String, List<Writable>, String>() {

			private static final long serialVersionUID = -3258858634428498358L;

			@Override
			public Tuple2<List<Writable>, String> call(String csv) throws Exception {
				TransformProcess transformProcess = TransformProcess.fromJson(transformProcessJsonBc.getValue());
				String tagCsv = csv.substring(0, csv.lastIndexOf(','));
				String value = csv.substring(csv.lastIndexOf(',') + 1);
				return new Tuple2<List<Writable>, String>(TransformerUtil.transform(transformProcess, tagCsv), value);
			}
		});
	}

	private static String rowToOriginalFeatureCsv(Row row, int opportunityDefaultIndex) {
		List<String> result = new LinkedList<>();
		for (int i = 0; i < row.size(); i++) {
			if (i == opportunityDefaultIndex) {
				result.add(sumOpportunity(String.valueOf(row.get(i))));
			} else {
				result.add(String.valueOf(row.get(i)));
			}
		}
		return StringUtils.join(result, ",");
	}
	
	public JavaRDD<LabeledPoint> transormForSparkMllibTrain() {
		JavaPairRDD<List<Writable>, String> featureAndLabels = transform(true);

		JavaRDD<LabeledPoint> result = featureAndLabels.map(new Function<Tuple2<List<Writable>, String>, LabeledPoint>() {

			private static final long serialVersionUID = 5940157377887080832L;

			@Override
			public LabeledPoint call(Tuple2<List<Writable>, String> featureAndLabel) throws Exception {
				double[] entryValues = new double[featureAndLabel._1.size()];
				for (int i = 0; i < featureAndLabel._1.size(); i++) {
					entryValues[i] = Double.valueOf(featureAndLabel._1.get(i).toString());
				}
				return new LabeledPoint(Double.valueOf(featureAndLabel._2), Vectors.dense(entryValues));
			}
		});
		LabeledPoint labeledPoint = result.first();
		logger.info("特征数量:{}", labeledPoint.features().size());
		return result;
	}

	public JavaPairRDD<String, Vector> transormForSparkMllibPredict() {
		JavaPairRDD<List<Writable>, String> featureAndRows = transform(false);

		return featureAndRows.mapToPair(new PairFunction<Tuple2<List<Writable>, String>, String, Vector>() {

			private static final long serialVersionUID = 4958937185395531711L;

			@Override
			public Tuple2<String, Vector> call(Tuple2<List<Writable>, String> featureAndRow) throws Exception {
				double[] entryValues = new double[featureAndRow._1.size()];
				for (int i = 0; i < featureAndRow._1.size(); i++) {
					entryValues[i] = Double.valueOf(featureAndRow._1.get(i).toString());
				}
				return new Tuple2<String, Vector>(featureAndRow._2, Vectors.dense(entryValues));
			}
		});
	}

	private TransformProcess getDataTransformProcess(Schema dataSchema, DataAnalysis dataAnalysis,
			StructType tagCsvSchema) {
		TransformProcess.Builder builder = new TransformProcess.Builder(dataSchema);
		for (int i = 0; i < tagCsvSchema.size(); i++) {
			StructField structField = tagCsvSchema.apply(i);
			if (structField.dataType().equals(DataTypes.StringType)) {
				if ("kcDefault".equals(structField.name())) {
					builder.transform(new CategoricalToMultiHotTransform(structField.name(), "~~"));
				} else {
					builder.transform(new DealWithUnknownStateCategoricalToMultiHotTransform(structField.name()));
				}
			} else if (structField.dataType().equals(DataTypes.DoubleType)) {
				builder.normalize(structField.name(), Normalize.Standardize, dataAnalysis);
			} else {
				new IllegalArgumentException("不支持-" + structField.name() + "-列类型:" + structField.dataType());
			}
		}
		return builder.build();
	}

	private Schema getDataSchema(DataFrame tagCsvDataFrame) {
		Schema.Builder builder = new Schema.Builder();
		StructType dataFrameSchema = tagCsvDataFrame.schema();
		for (int i = 0; i < dataFrameSchema.size(); i++) {
			StructField structField = dataFrameSchema.apply(i);
			if (structField.dataType().equals(DataTypes.StringType)) {
				List<String> states;
				if ("kcDefault".equals(structField.name())) {
					states = uniqueKc(tagCsvDataFrame);
				} else {
					states = uniqueColumnValue(tagCsvDataFrame, structField.name());
				}
				builder.addColumnCategorical(structField.name(), states);
			} else if (structField.dataType().equals(DataTypes.DoubleType)) {
				builder.addColumnDouble(structField.name());
			} else {
				new IllegalArgumentException("不支持-" + structField.name() + "-列类型:" + structField.dataType());
			}
		}
		return builder.build();
	}

	private List<String> uniqueKc(DataFrame tagCsvDataFrame) {
		return tagCsvDataFrame.select("kcDefault").toJavaRDD().flatMap(new FlatMapFunction<Row, String>() {

			private static final long serialVersionUID = -654449361417054004L;

			@Override
			public Iterable<String> call(Row row) throws Exception {
				return Arrays.asList(row.getString(0).split("~~"));
			}
		}).distinct().collect();
	}

	private List<String> uniqueColumnValue(DataFrame tagCsvDataFrame, String columnName) {
		Objects.requireNonNull(tagCsvDataFrame);
		Objects.requireNonNull(columnName);
		List<String> result = new ArrayList<>();
		DataFrame singleColumn = tagCsvDataFrame.select(columnName).distinct();
		Row[] rows = singleColumn.collect();
		for (Row row : rows) {
			result.add(row.get(0) == null ? "other" : row.getString(0));
		}
		return result;
	}

	public static List<String> uniqueColumnValue(JavaRDD<Row> singleColumnRows) {
		return singleColumnRows.map(new Function<Row, String>() {

			private static final long serialVersionUID = -7516780701061768973L;

			@Override
			public String call(Row row) throws Exception {
				return row.get(0) == null ? "other" : row.getString(0);
			}
		}).distinct().collect();
	}
}
