package com.liyuncong.learn.kddcup2010.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;

import scala.Tuple2;

/**
 * 
 * @author liyuncong
 *
 */
public class ArgumentUtil {
	private ArgumentUtil() {}
	
	public static Map<String, String> parseMainArgs(String[] args) {
		Map<String, String> result = new HashMap<>();
		if (args == null || args.length == 0) {
			return result;
		}
		
		for(String arg : args) {
			String[] kv = arg.split("=");
			result.put(kv[0], kv[1]);
		}
		
		return result;
	}
	
	/**
	 * 用sparkConf记录spark的配置信息，该配置信息可能会被用于优化spark运行
	 * @param conf
	 */
	public static Map<String, String> parseSparkConf(SparkConf conf) {
		Map<String, String> result = new HashMap<>();
		for(Tuple2<String, String> tuple2 : conf.getAll()) {
			result.put(tuple2._1, tuple2._2);
		}
		return result;
	}
}
