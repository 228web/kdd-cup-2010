package com.liyuncong.learn.kddcup2010.exploratorydataanalysis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.liyuncong.learn.kddcup2010.util.FileUtil;
import com.liyuncong.learn.kddcup2010.util.FileUtil.FileLineIterator;
import com.liyuncong.learn.kddcup2010.util.LinuxCommand;

/**
 * 
 * @author liyuncong
 *
 */
public class Coverage {
	private Coverage() {}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
		int total = 0;
		double noneBlankStatistic = 0;
		try(BufferedReader bufferedReader = FileUtil.getBufferedReader(DataFiles.algebra_2008_2009_train)) {
			FileLineIterator fileLineIterator = FileUtil.getFileLineIterator(bufferedReader);
			while (fileLineIterator.hasNext()) {
				String line = fileLineIterator.next();
				total++;
				String field = LinuxCommand.cut(line, "\t", 6);
				if (StringUtils.isNoneBlank(field)) {
					noneBlankStatistic++;
				}
			}
		}
		System.out.println("total:" + total);
		System.out.println("noneBlankStatistic:" + noneBlankStatistic);
		System.out.println("coverage:" + (noneBlankStatistic / total));
	}
	
}
