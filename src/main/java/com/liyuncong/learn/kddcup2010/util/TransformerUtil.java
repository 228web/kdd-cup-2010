package com.liyuncong.learn.kddcup2010.util;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.StringSplit;
import org.datavec.api.transform.DataAction;
import org.datavec.api.transform.Transform;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.transform.categorical.CategoricalToIntegerTransform;
import org.datavec.api.transform.transform.categorical.CategoricalToOneHotTransform;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.WritablesToStringFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author liyuncong
 *
 */
public class TransformerUtil implements Serializable{
	private static final long serialVersionUID = 6972895921845559654L;
	
	private static Logger logger = LoggerFactory.getLogger(TransformerUtil.class);

	private TransformerUtil() {}
	
	/**
	 * 
	 * @param cvs 由原始特征组成的csv
	 * @return 由特征组成的csv
	 * @throws Exception 
	 */
	public static List<Writable> transform(TransformProcess dataTransformProcess, String tagCsv) throws Exception {
		Objects.requireNonNull(tagCsv);
		
		List<Writable> writables = strintToWritable(tagCsv);
		
		return dataTransformProcess.execute(writables);
	}
	
	private static List<Writable> strintToWritable(String tagCsv) throws IOException, InterruptedException {
		Objects.requireNonNull(tagCsv);
		RecordReader recordReader = new CSVRecordReader();
		recordReader.initialize(new StringSplit(tagCsv));
		Collection<Writable> next = recordReader.next();
		if (next instanceof List)
            return (List<Writable>) next;
        return new ArrayList<>(next);
	}
	
	private static String writablesToString(List<Writable> writables) throws Exception {
		Objects.requireNonNull(writables);
		WritablesToStringFunction writablesToStringFunction = new WritablesToStringFunction(",");
		return writablesToStringFunction.call(writables);
	}
}
