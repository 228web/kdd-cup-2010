package com.liyuncong.learn.kddcup2010.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author liyuncong
 *
 */
public class LinuxCommand {
	private LinuxCommand() {
	}
	
	public static List<String> cut(List<String> lines, String delimiter, int...fieldsIndex) {
		List<String> result = new ArrayList<>();
		for(String line : lines) {
			result.add(cut(line, delimiter, fieldsIndex));
		}
		return result;
	}
	
	public static String cut(String line, String delimiter, int...fieldsIndex) {
		int fieldsIndexNum = fieldsIndex.length;
		String[] fields = line.split(delimiter);
		String[] lineResult = new String[fieldsIndexNum];
		for(int i = 0; i < lineResult.length; i++) {
			lineResult[i] = fields[fieldsIndex[i]];
		}
		return StringUtils.join(lineResult, delimiter);
	}
}
