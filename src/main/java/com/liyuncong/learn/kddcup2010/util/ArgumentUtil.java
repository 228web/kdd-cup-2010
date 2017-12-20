package com.liyuncong.learn.kddcup2010.util;

import java.util.HashMap;
import java.util.Map;

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
}
